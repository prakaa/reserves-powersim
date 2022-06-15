# This run of the model runs unit commitment followed by economic dispatch
using CSV
using Dates
using DataFrames
using Gurobi
using Logging
using Plots
using PowerSystems
using PowerSimulations
using Statistics
using StatsPlots
using TimeSeries

import MathOptInterface
const MOI = MathOptInterface


function attach_components!(sys::PowerSystems.System)
    @info "Attaching components to system"
    # zone
    nsw_zone = LoadZone("NSW-LoadZone", 0.0, 0.0)
    # buses
    nsw_bus = Bus(1, "NSW", "REF", nothing, nothing,
                  nothing, nothing, nothing, nsw_zone,
                  )
    # generators
    ## baseload coal, cost of $12/MW/hr to min load then $24/MW/hr
    bayswater = ThermalStandard(; name="Bayswater", available=true, status=true,
                                bus=nsw_bus, active_power=6000.0, reactive_power=0.0,
                                rating=1e4, active_power_limits=(min=1000.0, max=1e4),
                                reactive_power_limits=(min=-1.0, max=1.0),
                                ramp_limits=(up=20.67, down=15.33),
                                operation_cost=ThreePartCost(
                                    VariableCost([(1.2e4, 1e3), (1.32e5, 6e3)]),
                                     0.0, 0.0, 0.0
                                     ),
                                base_power=1.0, time_limits=(up=0.02, down=0.02),
                                prime_mover=PrimeMovers.ST, fuel=ThermalFuels.COAL,
                                )

    ## ccgt, cost of $0/MW/hr to min load, then $36/MW/hr then $48/MW/hr
    tallawarra = ThermalStandard(; name = "Tallawarra", available = true,
                                 status = true, bus = nsw_bus, active_power = 850.0,
                                 reactive_power = 0.0, rating = 6000.0,
                                 prime_mover = PrimeMovers.ST,
                                 fuel = ThermalFuels.NATURAL_GAS,
                                 active_power_limits = (min = 199.0, max = 6000.0),
                                 reactive_power_limits = (min = -1.0, max = 1.0),
                                 time_limits = (up = 4.0, down = 4.0),
                                 ramp_limits = (up = 6.0, down = 6.0),
                                 operation_cost = ThreePartCost(
                                     VariableCost(
                                         [(0.0, 199.0), (36.0 * (1000.0 - 199.0), 1000.0),
                                          (36.0 * (1000.0 - 199.0) + 48.0 * 2000.0,
                                          3000.0)]),
                                     0.0, 50.0, 50.0),
                                 base_power = 1.0,
    )

    ## VRE
    nsw_solar = RenewableDispatch(; name = "NSWSolar", available = true, bus = nsw_bus,
                                  active_power = 0.0,
                                  reactive_power = 0.0,
                                  rating = 800.0,
                                  prime_mover = PrimeMovers.PVe,
                                  power_factor= 1.0,
                                  base_power = 1.0,
                                  reactive_power_limits = (min = -1.0, max = 1.0),
                                  operation_cost = TwoPartCost(VariableCost(0.0), 0.0)
                                    )
    ## loads
    nsw_load = PowerLoad("NSWLoad", true, nsw_bus, nothing, 0.0, 0.0, 1.0, 1.0, 0.0)

    add_components!(sys, [nsw_zone, nsw_bus, bayswater, tallawarra,
                          nsw_solar, nsw_load])
end


function attach_timeseries!(sys_ed::PowerSystems.System, sys_uc::PowerSystems.System)
    """
    Aggregate 5-minute data to hourly data using mean for the hour.
    """
    function convert_rt_to_uc(time_array::TimeArray)
        hourly_data = values(collapse(time_array, hour, first, mean)[1:end])
        date_format = Dates.DateFormat("d/m/y H:M")
        hours = collect(DateTime("01/01/2019 00:00", date_format):
                                 Hour(1):
                        DateTime("31/12/2019 23:00", date_format))
        return TimeArray(hours, hourly_data)
    end
    # demand
    @info "Add demand timeseries"
    raw_demand = CSV.read(joinpath("data", "demand.csv"), DataFrame)
    ## add demand timeseries to ED
    date_format = Dates.DateFormat("d/m/y H:M")
    di_year = collect(DateTime("01/01/2019 00:00", date_format):
                      Dates.Minute(5):
                      DateTime("31/12/2019 23:55", date_format))
    nsw_demand_data = TimeArray(di_year, raw_demand[:, :nsw_demand])
    nsw_demand_ts = SingleTimeSeries("max_active_power", nsw_demand_data)
    ed_load = get_component(PowerLoad, sys_ed, "NSWLoad")
    add_time_series!(sys_ed, ed_load, nsw_demand_ts)
    ## add demand timeseries to UC from 01:00 on day 1 (8760 entries)
    nsw_demand_hourly = convert_rt_to_uc(nsw_demand_data)
    nsw_demand_uc = SingleTimeSeries("max_active_power", nsw_demand_hourly)
    uc_load = get_component(PowerLoad, sys_uc, "NSWLoad")
    add_time_series!(sys_uc, uc_load, nsw_demand_uc)

    # VRE generation
    @info "Add VRE timeseries"
    pv_gen = CSV.read("data/nsw_pv_manildra.csv_5mininterpolated.csv",
                      DataFrame)
    ## add PV generation timeseries
    pv_gen_data = TimeArray(di_year, pv_gen[:, :gen_mw])
    pv_gen_ts = SingleTimeSeries("max_active_power", pv_gen_data)
    ed_pv = get_component(RenewableDispatch, sys_ed, "NSWSolar")
    add_time_series!(sys_ed, ed_pv, pv_gen_ts)
    ## add PV hourly series to UC from 01:00 on day 1 (8760 entries)
    pv_gen_hourly = convert_rt_to_uc(pv_gen_data)
    pv_gen_uc = SingleTimeSeries("max_active_power", pv_gen_hourly)
    uc_pv = get_component(RenewableDispatch, sys_uc, "NSWSolar")
    add_time_series!(sys_uc, uc_pv, pv_gen_uc)

    # use SingleTimeSeries as forecasts
    @info "Specify ED as a single interval 5-minute problem"
    transform_single_time_series!(sys_ed, 1, Minute(5))
    @info "Specify UC as a 24 hour problem"
    transform_single_time_series!(sys_uc, 24, Hour(24))
end


function populate_ed_problem(sys_ed::PowerSystems.System,
                             solver::MOI.OptimizerWithAttributes)
    @info "Building ED Problem with device models"
    ed_problem_template = ProblemTemplate(NetworkModel(
        CopperPlatePowerModel, duals = [CopperPlateBalanceConstraint]
    ))
    set_device_model!(ed_problem_template, ThermalStandard, ThermalStandardDispatch)
    set_device_model!(ed_problem_template, PowerLoad, StaticPowerLoad)
    set_device_model!(ed_problem_template, RenewableDispatch, RenewableFullDispatch)
    problem = DecisionModel(ed_problem_template, sys_ed;
                            horizon=1,
                            optimizer=solver,
                            optimizer_solve_log_print=true,
                            name="ED", warm_start=false
                            )
    return problem
end


function populate_uc_problem(sys_uc::PowerSystems.System,
                             solver::MOI.OptimizerWithAttributes)
    @info "Building UC Problem with device models"
    uc_problem_template = ProblemTemplate()
    set_device_model!(uc_problem_template, ThermalStandard, ThermalStandardUnitCommitment)
    set_device_model!(uc_problem_template, PowerLoad, StaticPowerLoad)
    set_device_model!(uc_problem_template, RenewableDispatch, RenewableFullDispatch)
    problem = DecisionModel(uc_problem_template, sys_uc;
                            horizon=24,
                            optimizer=solver,
                            optimizer_solve_log_print=true,
                            name="UC", warm_start=false
                                )
    return problem
end

function run_simulation()
    output_dir = joinpath("built", "simple_uced")
    if !isdir(output_dir)
        mkpath(output_dir)
    end

    # create systems
    sys_UC = System(1.0;)
    sys_ED = System(1.0;)
    systems = (sys_UC, sys_ED)
    for sys in systems
        set_units_base_system!(sys, "NATURAL_UNITS")
        attach_components!(sys)
    end
    attach_timeseries!(sys_ED, sys_UC)

    solver = optimizer_with_attributes(Gurobi.Optimizer,
                                       "MIPGap" => 0.5, "OutputFlag" => 1, "GURO_PAR_DUMP" => 1)
    ed_problem = populate_ed_problem(sys_ED, solver)
    uc_problem = populate_uc_problem(sys_UC, solver)

    # test building operational problems
    for (problem, dir) in zip((ed_problem, uc_problem), ("ED", "UC"))
        build!(problem, output_dir = joinpath(output_dir, "stage-build", dir))
    end
    f = open("uced_thermalstandard.txt","w"); print(f, PowerSimulations.get_jump_model(uc_problem)); close(f)
    sim_models = SimulationModels(
        decision_models = [uc_problem, ed_problem],
    )

    # now assemble simulation sequence
    ## feedforward
    sim_sequence = SimulationSequence(
        models = sim_models,
        ini_cond_chronology = InterProblemChronology(),
        feedforwards = Dict(
            "ED" => [
                SemiContinuousFeedforward(
                    component_type=ThermalStandard,
                    source=OnVariable,
                    affected_values=[ActivePowerVariable]
                )
            ]
        )
    )

    sim = Simulation(
        name="EDUC-ThermalStandard",
        steps=4,
        models=sim_models,
        sequence=sim_sequence,
        simulation_folder=output_dir,
    )

    build!(sim; serialize=true, console_level=Logging.Info)
    @info "Executing simulation"
    execute!(sim; enable_progress_bar=true)
    return sys_UC, sys_ED, output_dir
end

try
    sys_UC, sys_ED, output_dir = run_simulation()
    results = SimulationResults(joinpath(output_dir, "EDUC-ThermalStandard"))
catch
    @error "Simulation error"
end

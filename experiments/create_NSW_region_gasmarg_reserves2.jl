# This run of the model sets all generation bar gas as zero cost with reserves
using CSV
using Cbc
using Dates
using DataFrames
using Ipopt
using Gurobi
using Logging
using TimeSeries
using PowerSystems
using PowerSimulations
using PowerGraphics

output_dir = joinpath("built", "dispatch_data")
if !isdir(output_dir)
    mkpath(output_dir)
end
raw_demand = CSV.read(joinpath("data", "demand.csv"), DataFrame)

# create system
sys = System(
    1.0;
)
set_units_base_system!(sys, "NATURAL_UNITS")


# zone
nsw_zone = LoadZone("NSW-LoadZone",
                    maximum(raw_demand[:, :nsw_demand]),
                    0.0
                    )
add_component!(sys, nsw_zone)

# buses
nsw_bus = Bus(1,
              "NSW", 
              "REF",
              nothing,
              nothing,
              nothing,
              nothing,
              nothing,
              nsw_zone,
              )

# generators
## baseload coal, cost of $12/MW/hr to min load then $24/MW/hr
bayswater = ThermalStandard(;
    name="Bayswater",
    available=true,
    status=true,
    bus=nsw_bus,
    active_power=0.0,
    reactive_power=0.0,
    #rating=2640.0,
    rating=6000.0,
    #active_power_limits=(min=1000.0, max=2545.0),
    active_power_limits=(min=1000.0, max=6000.0),
    reactive_power_limits=(min=-1.0, max=1.0),
    ramp_limits=(up=20.67, down=15.33),
    operation_cost=ThreePartCost(
        VariableCost([(1.2e4, 1e3), (1.32e5, 6e3)]),
        #(0.0, 10.0),
         0.0, 0.0, 0.0
         ),
    base_power=1.0,
    time_limits=(up=0.02, down=0.02),
    prime_mover=PrimeMovers.ST,
    fuel=ThermalFuels.COAL,
    )

## ccgt, cost of $0/MW/hr to min load, then $36/MW/hr then $48/MW/hr 
tallawarra = ThermalStandard(;
    name = "Tallawarra",
    available = true,
    status = true,
    bus = nsw_bus,
    active_power = 0.0,
    reactive_power = 0.0,
    #rating = 440.0,
    rating = 3000.0,
    prime_mover = PrimeMovers.ST,
    fuel = ThermalFuels.NATURAL_GAS,
    #active_power_limits = (min = 199.0, max = 395.0),
    active_power_limits = (min = 199.0, max = 3000.0),
    reactive_power_limits = (min = -1.0, max = 1.0),
    time_limits = (up = 4.0, down = 4.0),
    ramp_limits = (up = 6.0, down = 6.0),
    operation_cost = ThreePartCost(
        VariableCost([(0.0, 199.0), (36.0 * (1000.0 - 199.0), 1000.0),
                      (36.0 * (1000.0 - 199.0) + 48.0 * 2000.0, 3000.0)]),
        0.0, 50.0, 50.0),
    base_power = 1.0,
)

## peaker, cost of $200/MW/hr then $1000/MW/hr
peaker = ThermalMultiStart(;
    name = "Peaker",
    available = true,
    status = true,
    bus = nsw_bus,
    active_power = 0.0,
    reactive_power = 0.0,
    #rating = 440.0,
    rating = 1000.0,
    prime_mover = PrimeMovers.ST,
    fuel = ThermalFuels.NATURAL_GAS,
    active_power_limits = (min = 100.0, max = 1000.0),
    reactive_power_limits = (min = -1.0, max = 1.0),
    ramp_limits = (up = 200.0, down = 200.0),
    power_trajectory = (startup = 5.0, shutdown = 5.0),
    time_limits = (up = 0.0, down = 0.0),
    start_time_limits = (hot = 2.0, warm = 4.0, cold = 6.0),
    start_types = 1,
    operation_cost = ThreePartCost(
    # cost of $1000/hr
        VariableCost([(2e4, 1e2), (9.2e5, 1e3)]),
        1e3, 0.0, 0.0), 
        base_power = 1.0
    )

nsw_solar = RenewableDispatch(;
    name = "NSWSolar",
    available = true,
    bus = nsw_bus,
    active_power = 0.0,
    reactive_power = 0.0,
    rating = 800.0,
    prime_mover = PrimeMovers.PVe,
    power_factor= 1.0,
    base_power = 1.0,
    reactive_power_limits = (min = -1.0, max = 1.0),
    operation_cost = TwoPartCost(VariableCost(0.0), 0.0)
)

# loads
nsw_load = PowerLoad(
    "NSWLoad",
    true,
    nsw_bus,
    nothing,
    0.0,
    0.0,
    1.0,
    1.0,
    0.0
)
add_components!(sys, [nsw_bus, tallawarra, bayswater, peaker, nsw_solar, nsw_load])

# reserves
## bug in StaticReserve - add static requirement for VariableReserve
or = VariableReserve{ReserveUp}("OR", true, 5.0, 150.0)
add_component!(sys, or)
set_services!(bayswater, [or])
set_services!(tallawarra, [or])
set_services!(peaker, [or])

# add load timeseries
date_format = Dates.DateFormat("d/m/y H:M")
di_year = collect(DateTime("01/01/2019 00:05", date_format):
                  Dates.Minute(5):
                  DateTime("01/01/2020 00:00", date_format))
nsw_demand_data = TimeArray(di_year, raw_demand[:, :nsw_demand])
nsw_demand_ts = SingleTimeSeries("max_active_power", nsw_demand_data)
add_time_series!(sys, nsw_load, nsw_demand_ts)

# add PV generation timeseries
pv_gen = CSV.read("data/nsw_pv_manildra.csv_5mininterpolated.csv", 
                  dateformat="yyyy-mm-dd H:M:S", DataFrame)
pv_gen_data = TimeArray(pv_gen[:, :Datetime], pv_gen[:, :gen_mw])
pv_gen_ts = SingleTimeSeries("max_active_power", pv_gen_data)
add_time_series!(sys, nsw_solar, pv_gen_ts)

# add reserve timeseries
add_time_series!(
    sys, or, SingleTimeSeries("requirement", TimeArray(di_year, ones(length(di_year))))
    )

# use SingleTimeSeries as forecasts
transform_single_time_series!(sys, 1, Minute(5))

solver = optimizer_with_attributes(Gurobi.Optimizer)
#ed_problem_template = template_economic_dispatch()
ed_problem_template = OperationsProblemTemplate()
set_device_model!(ed_problem_template, ThermalStandard, ThermalDispatch)
set_device_model!(ed_problem_template, ThermalMultiStart, ThermalDispatch)
set_device_model!(ed_problem_template, PowerLoad, StaticPowerLoad)
set_device_model!(ed_problem_template, RenewableDispatch, RenewableConstantPowerFactor)
set_service_model!(ed_problem_template, VariableReserve{ReserveUp}, RampReserve)
## dual for reserves - :requirement__VariableReserve_ReserveUp
problem = OperationsProblem(ed_problem_template, sys;
                            optimizer=solver, 
                            constraint_duals=[:CopperPlateBalance,
                                              :requirement__VariableReserve_ReserveUp],
                            horizon=1,
                            balance_slack_variables=true,
                            services_slack_variables=true
                            )
# test op problem
build!(problem, output_dir = joinpath("built", "problemdata"))

sim_problem = SimulationProblems(ED=problem)
sim_sequence = SimulationSequence(
    problems=sim_problem,
    intervals=Dict("ED" => (Minute(5), Consecutive())),
    ini_cond_chronology = IntraProblemChronology(),
)
sim = Simulation(
    name="economic_dispatch",
    steps=288,
    problems=sim_problem,
    sequence=sim_sequence,
    simulation_folder=output_dir,
)

build!(sim; serialize=true, console_level=Logging.Info)
execute!(sim;)
#execute!(sim; exports="export.json")

results = SimulationResults(joinpath(output_dir, "economic_dispatch"))
ed_results = get_problem_results(results, "ED")
timestamps = get_realized_timestamps(ed_results)
variables = read_realized_variables(ed_results)
generation = get_generation_data(ed_results)
reserves = get_service_data(ed_results)

gr()
set_system!(ed_results, sys)
p = plot_fuel(ed_results, stack=true);
savefig(p, "results/fuel_gasmarg_reserves2.png")
duals = read_realized_duals(ed_results)
prices = duals[:CopperPlateBalance]
reserves_prices = duals[:requirement__VariableReserve_ReserveUp][:, 2]

if !isdir("results")
    mkdir("results")
end
price_analysis = hcat(prices, reserves_prices, 
                      reserves.data[:OR__VariableReserve_ReserveUp][!, 2:4],
                      generation.data[:P__ThermalStandard][!, :Bayswater],
                      generation.data[:P__ThermalStandard][!, :Tallawarra],
                      generation.data[:P__ThermalMultiStart][!, :Peaker],
                      makeunique=true)
DataFrames.rename!(price_analysis, ["Datetime", "CopperPlateBalance",
                                    "ReserveDual", "bayswater_reserves",
                                    "peaker_reserves", "tallawarra_reserves", 
                                    "bayswater_mw", "tallawarra_mw", "peaker_mw"])
CSV.write("results/prices_with_reserves2.csv", price_analysis)
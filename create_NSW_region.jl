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
bayswater = ThermalStandard(;
    name="Bayswater",
    available=true,
    status=true,
    bus=nsw_bus,
    active_power=2545.0,
    reactive_power=0.0,
    rating=2640.0,
    active_power_limits=(min=1000.0, max=2545.0),
    reactive_power_limits=(min=-1.0, max=1.0),
    ramp_limits=(up=20.67, down=15.33),
    operation_cost=ThreePartCost(
        (0.0, 10.0),
         0.0, 200.0, 200.0
         ),
    base_power=1.0,
    time_limits=(up=0.02, down=0.02),
    prime_mover=PrimeMovers.ST,
    fuel=ThermalFuels.COAL,
    )

tallawarra = ThermalStandard(;
    name = "Tallawarra",
    available = true,
    status = true,
    bus = nsw_bus,
    active_power = 395.0,
    reactive_power = 0.0,
    rating = 440.0,
    prime_mover = PrimeMovers.ST,
    fuel = ThermalFuels.NATURAL_GAS,
    active_power_limits = (min = 199.0, max = 395.0),
    reactive_power_limits = (min = -1.0, max = 1.0),
    time_limits = (up = 4.0, down = 4.0),
    ramp_limits = (up = 6.0, down = 6.0),
    operation_cost = ThreePartCost(
        (0.0, 100.0),
        0.0, 50.0, 50.0),
    base_power = 1.0,
)

nsw_solar = RenewableDispatch(;
    name = "NSWSolar",
    available = true,
    bus = nsw_bus,
    active_power = 200.0,
    reactive_power = 0.0,
    rating = 200.0,
    prime_mover = PrimeMovers.PVe,
    power_factor=1.0,
    base_power = 1.0,
    reactive_power_limits = (min = -1.0, max = 1.0),
    operation_cost = TwoPartCost(0.0, 1.0)
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

# reserves
# bug in StaticReserve - add static requirement for VariableReserve
or = VariableReserve{ReserveUp}("OR", true, 300.0, 50.0)

add_components!(sys, [nsw_bus, tallawarra, bayswater, nsw_solar, nsw_load, or])
set_services!(bayswater, [or])
set_services!(tallawarra, [or])

# add load timeseries
date_format = Dates.DateFormat("d/m/y H:M")
di_year = collect(DateTime("01/01/2019 00:05", date_format):
                  Dates.Minute(5):
                  DateTime("01/01/2020 00:00", date_format))
nsw_demand_data = TimeArray(di_year, raw_demand[:, :nsw_demand])
nsw_demand = SingleTimeSeries("max_active_power", nsw_demand_data)
add_time_series!(sys, nsw_load, nsw_demand)

# add reserve timeseries
add_time_series!(
    sys, or, SingleTimeSeries("requirement", TimeArray(di_year, ones(length(di_year))))
    )

# add VRE forecast timeseries
add_time_series!(
    sys, nsw_solar, 
    SingleTimeSeries("max_active_power", TimeArray(di_year, ones(length(di_year))))
    )

# use SingleTimeSeries as forecasts
transform_single_time_series!(sys, 1, Minute(5))

solver = optimizer_with_attributes(Gurobi.Optimizer)
ed_problem_template = OperationsProblemTemplate()
set_device_model!(ed_problem_template, ThermalStandard, ThermalDispatch)
set_device_model!(ed_problem_template, PowerLoad, StaticPowerLoad)
set_device_model!(ed_problem_template, RenewableDispatch, RenewableConstantPowerFactor)
set_service_model!(ed_problem_template, VariableReserve{ReserveUp}, RampReserve)
problem = OperationsProblem(ed_problem_template, sys;
                            optimizer=solver, 
                            constraint_duals=[:CopperPlateBalance, 
                                              :requirement__VariableReserve_ReserveUp],
                            horizon=1, 
                            balance_slack_variables=true
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
    steps=100,
    problems=sim_problem,
    sequence=sim_sequence,
    simulation_folder=output_dir
)

build!(sim; serialize=true, console_level=Logging.Debug)
execute!(sim)

results = SimulationResults(joinpath(output_dir, "economic_dispatch"))
ed_results = get_problem_results(results, "ED")
timestamps = get_realized_timestamps(ed_results)
variables = read_realized_variables(ed_results)
generation = get_generation_data(ed_results)
reserves = get_service_data(ed_results)

gr()
set_system!(ed_results, sys)
plot_fuel(ed_results, stack=true)
read_realized_duals(ed_results)[:CopperPlateBalance]
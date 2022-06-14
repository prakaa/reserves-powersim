using CSV
using Dates
using DataFrames
using Gurobi
using Logging
using TimeSeries
using PowerSystems
using PowerSimulations
using PowerGraphics

output_dir = joinpath("built", "simple_ed")
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
    active_power=0.0,
    reactive_power=0.0,
    rating=6000.0,
    active_power_limits=(min=1000.0, max=6000.0),
    reactive_power_limits=(min=-1.0, max=1.0),
    ramp_limits=(up=20.67, down=15.33),
    operation_cost=ThreePartCost(
        0.0, 0.0, 0.0, 0.0
         ),
    base_power=1.0,
    time_limits=(up=0.02, down=0.02),
    prime_mover=PrimeMovers.ST,
    fuel=ThermalFuels.COAL,
    )

# price is piecewise linear with slopes ($/MW/hr) of 12, then 48
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
        VariableCost([(100.0 * 12.0, 100.0),
                      ((3000.0-100.0) * 48.0 + 100.0 * 12.0, 3000.0)]),
        0.0, 50.0, 50.0),
    base_power = 1.0,
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
add_components!(sys, [nsw_zone, nsw_bus, tallawarra, bayswater, nsw_solar, nsw_load])


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

# use SingleTimeSeries as forecasts
transform_single_time_series!(sys, 1, Minute(5))

# problem template and device formulations
ed_problem_template = OperationsProblemTemplate()
set_device_model!(ed_problem_template, ThermalStandard, ThermalDispatch)
set_device_model!(ed_problem_template, PowerLoad, StaticPowerLoad)
set_device_model!(ed_problem_template, RenewableDispatch, RenewableConstantPowerFactor)

# build operations problem
solver = optimizer_with_attributes(Gurobi.Optimizer)
problem = OperationsProblem(ed_problem_template, sys;
                            optimizer=solver,
                            constraint_duals=[:CopperPlateBalance],
                            horizon=1,
                            balance_slack_variables=true
                            )
# test op problem
# running solve on this runs a single step of the problem
build!(problem, output_dir = joinpath(output_dir, "ed-build"))

# build simulation sequence, which links steps of the problem
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

# extract and output/plot results
results = SimulationResults(joinpath(output_dir, "economic_dispatch"))
ed_results = get_problem_results(results, "ED")
timestamps = get_realized_timestamps(ed_results)
variables = read_realized_variables(ed_results)
generation = get_generation_data(ed_results)
if !isdir("results")
    mkdir("results")
end
set_system!(ed_results, sys)
p = plot_fuel(ed_results, stack=true)
savefig(p, "results/fuel_plot_simple_ed.png")
prices = read_realized_duals(ed_results)[:CopperPlateBalance]
price_analysis = hcat(prices,
                      generation.data[:P__ThermalStandard][!, [:Bayswater,
                                                               :Tallawarra]])
CSV.write("results/values_simple_ed.csv", price_analysis)

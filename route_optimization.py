import numpy as np
import pandas as pd

from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

# We're using Google's Opensource OR tools library for optimization functions
# Link: https://developers.google.com/optimization/routing


def create_data_model(dist_mat, delivery_mat, num_vehicles, capacity_mat, vehicle_cap):
    """
    Stores the data for the problem.
    We have modified it to take in the distance matrix we generated and use other 
    constraints such as the capacity constraint and vehicle capacity cap.
    Input:
        dist_mat (2D list): the distance matrix we got from generate_matrix.py
        delivery_mat (2D list): the distance matrix we will generate below
        num_vehicles (int): Max number of buses
        capacity_mat (2D list): Passenger capacity at each bus stop
        vehicle_cap (int): The max capacity of buses
    Returns:
        A data dictionary with all the above as key-value pairs
    
    """
    data = {}
    data['distance_matrix'] = dist_mat
    data['pickups_deliveries'] = delivery_mat
    data['num_vehicles'] = num_vehicles
    data['depot'] = 0
    data['demands'] = capacity_mat
    data['vehicle_capacities'] = [vehicle_cap]*num_vehicles
    return data


def print_solution(data, manager, routing, solution):
    """Prints solution on console."""
    total_distance = 0
    total_load = 0
    for vehicle_id in range(data['num_vehicles']):
        index = routing.Start(vehicle_id)
        plan_output = 'Route for vehicle {}:\n'.format(vehicle_id)
        route_distance = 0
        route_load = 0
        while not routing.IsEnd(index):
            node_index = manager.IndexToNode(index)
            route_load += data['demands'][node_index]
            plan_output += ' {0} Load({1}) -> '.format(node_index, route_load)
            previous_index = index
            index = solution.Value(routing.NextVar(index))
            route_distance += routing.GetArcCostForVehicle(
                previous_index, index, vehicle_id)
        plan_output += ' {0} Load({1})\n'.format(manager.IndexToNode(index),
                                                 route_load)
        plan_output += 'Distance of the route: {}00m\n'.format(route_distance)
        plan_output += 'Load of the route: {}\n'.format(route_load)
        print(plan_output)
        total_distance += route_distance
        total_load += route_load
    print('Total distance of all routes: {}00m'.format(total_distance))
    print('Total load of all routes: {}'.format(total_load))

def run_optimizer(dist_mat, delivery_mat, num_vehicles, capacity_mat, vehicle_cap):
    
    """Entry point of the program."""
    # Instantiate the data problem.
    data = create_data_model(dist_mat, delivery_mat, num_vehicles, capacity_mat, vehicle_cap)

    # Create the routing index manager.
    manager = pywrapcp.RoutingIndexManager(len(data['distance_matrix']),
                                            data['num_vehicles'], data['depot'])

    # Create Routing Model.
    routing = pywrapcp.RoutingModel(manager)
    
    def demand_callback(from_index):
        """Returns the demand of the node."""
        # Convert from routing variable Index to demands NodeIndex.
        from_node = manager.IndexToNode(from_index)
        return data['demands'][from_node]

    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(demand_callback_index,0,data['vehicle_capacities'], True,'Capacity')


    # Define cost of each arc.
    def distance_callback(from_index, to_index):
        """Returns the manhattan distance between the two nodes."""
        # Convert from routing variable Index to distance matrix NodeIndex.
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return data['distance_matrix'][from_node][to_node]

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # Add Distance constraint.
    dimension_name = 'Distance'
    routing.AddDimension(
        transit_callback_index,
        0,  # no slack
        1000,  # vehicle maximum travel distance
        True,  # start cumul to zero
        dimension_name)
    distance_dimension = routing.GetDimensionOrDie(dimension_name)
    distance_dimension.SetGlobalSpanCostCoefficient(100)

    # Define Transportation Requests.
    for request in data['pickups_deliveries']:
        pickup_index = manager.NodeToIndex(request[0])
        delivery_index = manager.NodeToIndex(request[1])
        routing.AddPickupAndDelivery(pickup_index, delivery_index)
        routing.solver().Add(
            routing.VehicleVar(pickup_index) == routing.VehicleVar(
                delivery_index))
        routing.solver().Add(
            distance_dimension.CumulVar(pickup_index) <=
            distance_dimension.CumulVar(delivery_index))

    # Setting first solution heuristic.
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION)

    # Solve the problem.
    solution = routing.SolveWithParameters(search_parameters)

    # Print solution on console.
    if solution:
        print_solution(data, manager, routing, solution)
    
    return (solution, routing, manager)

def get_routes(solution, routing, manager):
  """Get vehicle routes from a solution and store them in an array."""
  # Get vehicle routes and store them in a two dimensional array whose
  # i,j entry is the jth location visited by vehicle i along its route.
  routes = []
  for route_nbr in range(routing.vehicles()):
    index = routing.Start(route_nbr)
    route = [manager.IndexToNode(index)]
    while not routing.IsEnd(index):
      index = solution.Value(routing.NextVar(index))
      route.append(manager.IndexToNode(index))
    routes.append(route)
  return routes


def main(dist_arr_file, cap_arr_file, num_test, num_vehicles, vehicle_cap):
    dist_mat_arr = np.load(dist_arr_file)
    dist_mat = dist_mat_arr[:2*num_test+1,:2*num_test+1].tolist()
    capacity_mat = np.load(cap_arr_file).tolist()
    capacity_mat = capacity_mat[:(2*num_test+1)]

    delivery_mat = []
    for i in range(1,2*num_test+1,2):
        delivery_mat.append([i,(i + 1)])
    
    solution, routing, manager = run_optimizer(dist_mat, delivery_mat, num_vehicles, capacity_mat, vehicle_cap)
    routes = get_routes(solution, routing, manager)
    
    return routes

if __name__ == '__main__':
    routes = main('distance_arr_100.npy', 'capacity_arr_100.npy', num_test = 100, num_vehicles = 15, vehicle_cap = 30)
    # save routes as np
    routes_data = np.array(routes)
    np.save('routes_data', routes_data)

import haversine
import clean_data
import pandas as pd

def cluster(merge_dict, eligible_routes):
    '''
    Cluster by dropping mergeable routes after merging the frequency to the 
    route with higher frequency
    '''

    dropped = []
    for item, values in merge_dict.items():
        if item in eligible_routes.index.values:
            for value in values:
                if value in eligible_routes.index.values:
                    eligible_routes.at[item,
                                       'Freq'] += eligible_routes.at[value,
                                                                     'Freq']
                    eligible_routes.drop(index=value, inplace=True)
                    dropped.append(value)
    print("Number of dropped taxi routes (after merging):", len(dropped), '\n')
    print(
        "Number of routes available in the city for pooling in this hour \
    after optimization:", eligible_routes.shape[0], '\n')

    return eligible_routes.reset_index(drop=True)

def get_merge_dict(routes):
    '''
    Gets a dict that contains key: main route
    values: index of routes that can be merged to main route
    '''

    merge_dict = {}

    for given_route in routes.itertuples():
        if given_route[0] > 0:
            for pred_routes in routes.itertuples():
                if (given_route.pickup_latlon in pred_routes.pickup_nbors
                    ) and (given_route.dropoff_latlon
                           in pred_routes.dropoff_nbors):
                    if given_route[0] == pred_routes[0]:
                        break
                    else:
                        merge_dict[pred_routes[0]] = merge_dict.get(
                            pred_routes[0], [])
                        merge_dict[pred_routes[0]] += [given_route[0]]

    return merge_dict

def walkable_nbors(series, max_dist):
    '''
    Get the walkable neighbors from each latlong in the given 
    pd series of lat_longs
    '''

    n_routes = len(series)
    walkable_nbors = []
    for i in range(n_routes):
        walkable_neighbors = list(
            set(walking_distance_points(series[i], series, max_dist)))
        walkable_nbors.append(walkable_neighbors)

    return walkable_nbors

def walking_distance_points(point, series, walking_distance):
    '''
    Given a point tuple(lat, long) and a series of points list of 
    tuples(lat,long), calculates the distances between them in kms
    Returns the points in the series that are within walking distance
    This is not actual distance but an "as the crow flies" haversine distance
    '''

    walking_dist_points = []
    for b_point in series:
        if haversine.haversine(point, b_point) <= walking_distance:
            walking_dist_points.append(b_point)

    return walking_dist_points

def get_trip_dist(eligible_routes, d):
    '''
    Returns the list of trip distance in kilometers by looking up the 
    original dataframe. The return object is a list
    '''

    trip_dist = []
    for route in eligible_routes.itertuples():
        for original in d.itertuples():
            if (route.pickup_latitude == original.pickup_latitude) and (
                    route.pickup_longitude == original.pickup_longitude):
                if (route.dropoff_latitude == original.dropoff_latitude) and (
                        route.dropoff_longitude == original.dropoff_longitude):
                    distance = original.trip_miles * 1.6
                    trip_dist.append(distance)
                    break
    return trip_dist

def generate_clusters(n, h, max_dist, number_working_days):

    # Reading into the data and slicing in for the hour
    data = pd.read_csv("cleaned_data.csv", parse_dates=[['start_date', 'start_time'], ['end_date', 'end_time']])
    
    data = data.sort_values(['start_date_start_time']).reset_index(drop=True)
    hourly_groups = data.groupby(by=[data.start_date_start_time.map(lambda x: x.hour)])
    hourlydata = {}
    for key, df in hourly_groups:
        hourlydata[key] = df

    # Preprocessing of data based on the hour and other parameters
    d = hourlydata[h]
    df = d.groupby(['pickup_latitude', 'pickup_longitude', 'dropoff_latitude',
                    'dropoff_longitude']).size().reset_index(name="Freq")
    eligible_routes = df[df.Freq > n].sort_values(by='Freq', ascending=False, ignore_index=True)

    # Further processing on data
    eligible_routes['pickup_latlon'] = list(
        zip(eligible_routes.pickup_latitude, eligible_routes.pickup_longitude))
    eligible_routes['dropoff_latlon'] = list(
        zip(eligible_routes.dropoff_latitude,
            eligible_routes.dropoff_longitude))
    eligible_routes['trip_distance_kms'] = get_trip_dist(eligible_routes, d)
    eligible_routes['pickup_nbors'] = walkable_nbors(
        eligible_routes.pickup_latlon, max_dist)
    eligible_routes['dropoff_nbors'] = walkable_nbors(
        eligible_routes.dropoff_latlon, max_dist)

    # Merge and cluster the routes based on neighboring pickup and dropoffs
    merge_dict = get_merge_dict(eligible_routes)
    number_of_original_routes = eligible_routes.shape[0]
    print("Unique routes (pickups, dropoffs) chosen for optimization:",
          eligible_routes.shape[0], '\n')
    clustered_routes = cluster(merge_dict, eligible_routes)
    trips_covered = clustered_routes.Freq.sum()
    print("Number of trips covered in the optimized routes:", trips_covered,
          '\n')
    clustered_routes["trips_per_day"] = clustered_routes.Freq // \
        number_working_days

    # Return only required columns after slicing
    clusters = clustered_routes.loc[:, [
        'pickup_latlon', 'dropoff_latlon', 'pickup_nbors', 'dropoff_nbors',
        'Freq', 'trips_per_day', 'trip_distance_kms'
    ]]
    
    return clusters

if __name__ == "__main__":
    clusters = generate_clusters(20, 8, 0.5, 12)
    clusters.to_csv("clustered_data.csv", index = False)
    print("Data Clustering Complete ..........")

import pandas as pd
import numpy as np
import datetime
import haversine
from sklearn.cluster import KMeans

# README INSTRUCTIONS TO RUN THIS FILE IN VSCODE

# 1. Copy ot the folder where the cleaned up data exists
# 2. Start ipython in terminal and load usual stuff
# 3. import this File and reset he name of csv_file_location to where the file resides in your folder
# 4. then run the command
# 5. clusters = clustering.go(csv_file_location, n, h, max_dist)
# 6. ensure that you have declared the file location, n, h and max_dist (maximum
# walking distance) in ipython before running step 5
# 7. clusters is a dataframe. Each row is a route

# END OF INSTRUCTIONS

# DELETE THE ABOE AND BELOW COMMENTS BEFORE SUBMISSIONS
# ALSO DELETE THE PRINT STATEMENTS THAT IS PUT TO HELP DEBUGGING
# %load_ext autoreload
# %autoreload 2
# import clustering
# import pandas as pd
# import numpy as np
# import datetime
# import haversine

# Parameters to be called from outside
# n = 20
# h = 8
# max_dist = 1.5
# csv_file_location = "/Users/tiru/Documents/Documents_new/CS2/Project/clean_data.csv"
# End of parameters to be called from outside


def go(csv_file_location, n, h, max_dist):
    '''
    main function
    '''

    # Main processing area - to be merged into main function

    data = pd.read_csv(csv_file_location,
                       parse_dates=[['start_date', 'start_time'],
                                    ['end_date', 'end_time']])
    data = data.iloc[:, :-8]
    data = data.sort_values(['start_date_start_time']).reset_index(drop=True)

    # Split data on hourly basis
    hourly_groups = data.groupby(
        by=[data.start_date_start_time.map(lambda x: x.hour)])
    hourlydata = {}
    for key, df in hourly_groups:
        hourlydata[key] = df

    d = hourlydata[h]

    # KMeans

    km = KMeans(n_clusters=number_of_clusters)
    y = km.fit(d[['pickup_longitude', 'pickup_latitude',
                  'dropoff_longitude', 'dropoff_latitude']])
    y_predicted = y.predict(
        d[['pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude']])
    d["cluster"] = y_predicted
    centers = y.cluster_centers_
    centers_df = pd.DataFrame(centers, columns=[
                              'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude'])
    print(centers_df)

    d = d.reset_index(drop=True)

# Find walking distances

    # centers_dict
    d["pickup_km_dist_to_nearest_clustercenter"] = get_walk_distance(
        d.pickup_longitude, d.pickup_latitude, d.cluster, centers_df, 1)
    d["dropoff_km_dist_to_nearest_clustercenter"] = get_walk_distance(
        d.dropoff_longitude, d.dropoff_latitude, d.cluster, centers_df, 0)
    print("orignal shape:", d.shape)
    d["trip_distance"] = get_trip_dist(
        list(zip(d.pickup_latitude, d.pickup_longitude)), list(zip(d.dropoff_latitude, d.dropoff_longitude)))

    # Drop rows where walking is too much i.e. > max_dist

    d = d[d.pickup_km_dist_to_nearest_clustercenter <= max_dist]
    d = d[d.dropoff_km_dist_to_nearest_clustercenter <= max_dist]

    df = d.groupby("cluster").size().reset_index(name="Freq")
    print(df)
    eligible_clusters = df[df.Freq > n].sort_values(
        by='Freq', ascending=False, ignore_index=True)

    print(eligible_clusters.head())
    print("shape before merge", eligible_clusters.shape)

    # Merge/Join clusters with center details(lat/longs)

    clusters = eligible_clusters.merge(
        centers_df, how='left', left_on='cluster', right_index=True)
    clusters = clusters[[
        'cluster', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude', 'Freq']]
    clusters['trips_per_day'] = clusters.Freq//number_of_working_days
    print("After join:", clusters.head())
    print("Shaope after merge", clusters.shape)

    # Return data for further processing to next stage of project
    print("Stored the output file at: ", path_to_store)
    clusters.to_csv(path_to_store
                    )
    print(clusters.head())
    print(clusters.shape)
    return clusters


def get_trip_dist(series1, series2):
    '''
    Given a series list of tuples(lat, long) and a series of points list of tuples(lat,long), 
    calculates the distances between them in kms
    Returns the points in the series that are within walking distance
    '''

    trip_dist = []
    n = len(series1)
    for i in range(n):
        distance = haversine.haversine(series1[i], series2[i])
        r_distance = round(distance, 3)
        trip_dist.append(r_distance)

    return trip_dist


def get_walk_distance(long, lat, cluster_no, centers_df, pickup):
    '''
    long, lat self exp
    lst is of format [pickup long, pickup lat, dropoff long, dropoff lat]
    d.dropoff_longitude, d.dropoff_latitude, d.cluster, centers_df)
    '''

    dist_list = []
    n = len(long)
    for i in range(n):
        index = cluster_no[i]
        latlongs = centers_df.iloc[index]
        point1 = (lat[i], long[i])
        if pickup:
            point2 = (latlongs[1], latlongs[0])  # pickup latlongs
        else:
            point2 = (latlongs[3], latlongs[2])  # dropoff latlongs

        distance = haversine.haversine(point1, point2)
        r_distance = round(distance, 3)
        dist_list.append(r_distance)
    return dist_list


if __name__ == "__main__":
    usage = "create clusters"
    csv_file_location = "data/clean_data.csv"
    path_to_store = "data/clusterswithmldf.csv"
    n = 20
    h = 8
    max_dist = 1.5  # kms in haversine
    number_of_working_days = 20
    number_of_clusters = 200

    go(csv_file_location, n, h, max_dist)

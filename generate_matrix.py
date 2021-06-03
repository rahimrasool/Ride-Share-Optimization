import numpy as np
import pandas as pd
import requests

def get_data(file):
    '''
    This function will fetch the data that was produced after the clustering
    stage and perform some pre-processing steps

    Inputs:
        file (string): file path of the file to fetch
    Returns:
        clus_data (dataframe): The processed dataframe from this function    
    '''

    clus_data = pd.read_csv(file)
    # removing routes with large number of passenger to simplify processing
    clus_data = clus_data.drop(clus_data[clus_data.trips_per_day>30].index)
    clus_data.drop(['pickup_nbors','dropoff_nbors','Freq'], \
    axis = 1, inplace = True)
    # Change location data from string to tuples of float
    clus_data['pickup_latlon'] = clus_data['pickup_latlon'].apply(lambda x: \
    tuple(map(float, x[1:-1].split(', '))))
    clus_data['dropoff_latlon'] = clus_data['dropoff_latlon'].apply(lambda x: \
    tuple(map(float, x[1:-1].split(', '))))
    return clus_data

def return_dist(a, b):
    '''
    This function will return the distance between 2 location

    Inputs:
        a, b (tuple): tuple of latitude and longitude cordinates
    Returns:
        dist (integer): the distance calculated through mapbox API in meters     
    '''

    # The API Key is Public
    token = "pk.eyJ1IjoicmFoaW1yYXNvb2wiLCJhIjoiY2twZnR0aTYyMGFvNTJwcXBnNGZpM3AybCJ9.976YcOT1SrhjSyolq3ijsg"
    head = "https://api.mapbox.com/directions/v5/mapbox/driving/"
    tail = "?annotations=distance&access_token=" #Add token at the end of this
    request = head + str(a[1]) + ',' + str(a[0]) + ';' + str(b[1]) + ',' + str(b[0]) \
    + tail + token
    r = requests.get(request)
    # Add status code verification
    output = r.json()
    dist = int(output['routes'][0]['distance']/100)
    if dist == 1:
        return 0
    else:
        return dist

def avg_center(clus_data, num):
    '''
    This function helps in getting the best starting point for all buses
    as required by the optimization tool. This is the central point
    called the 'depo'

    Input:
        clus_data (dataframe): the dataframe with all taxi rides information
        num (integer): total number of rows we're using (we're not using all)
    Returns:
        A tuple with latitude and longitude point respectively
    '''
    # average points for depo
    avg_lat = 0
    avg_long = 0
    for (i, pi, di, tripsi, disti) in clus_data[:num].itertuples():
        avg_lat += pi[0] + di[0]
        avg_long += pi[1] + di[1]
    return (avg_lat/200, avg_long/200) 

def get_distance_matrices(clus_data, center, num):
    '''
    This function builds a 200x200 distance matrix that is used by 
    the route optimization tool. It saves it as a numpy array.

    Input:
        clus_data (dataframe): the dataframe with all taxi rides information
        center (tuple): the coordinates of the center point
        num (integer): total number of rows we're using (we're not using all)
    Returns:
        Saves dist_mat and pickup_demand as numpy array in main directory
    '''
    c = center
    depo = [0]
    dist_mat = []
    pickup_demand = [0]
    dist_mat.append(depo)
    for (i, pi, di, tripsi, disti) in clus_data[:num].itertuples():
        pickup = []
        pickup.append(return_dist(pi,c))
        depo.append(return_dist(pi,c))
        dropoff = []
        dropoff.append(return_dist(di,c))
        depo.append(return_dist(di,c))
        for (j, pj, dj, tripsj, distj) in clus_data[:num].itertuples():
            pickup.append(return_dist(pi,pj))
            pickup.append(return_dist(pi,dj))
            dropoff.append(return_dist(di,pj))
            dropoff.append(return_dist(di,dj))
    
        dist_mat.append(pickup)
        dist_mat.append(dropoff)
        pickup_demand.append(tripsi)
        pickup_demand.append(-tripsi)
    
    dist_arr = np.array(dist_mat)
    np.save('distance_arr_100', dist_arr)
    capacity = np.array(pickup_demand)
    np.save('capacity_arr_100', capacity)

if __name__ == '__main__':
    data = get_data("clustered_data.csv")
    center = avg_center(data, 100)
    get_distance_matrices(data, center, num = 100)
    ## Save dataframe as csv
    data.to_csv('clustered_data_updated.csv')
    print("200x200 Distanc matrix has been successfully created")

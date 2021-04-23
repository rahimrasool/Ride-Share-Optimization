import numpy as np
import pandas as pd

import folium
import random
import requests

def get_loc(routes, clus_data, center):
    '''
    This function creates 2 matrices called locations and counts that assists in
    generating maps. Routes is a list of routes where each route is a list of 
    coordinates on that route. We generated it through our optimization function.
    Moreover, counts keeps track of passengers at every location for every route.

    Input:
        routes (list):  the optimized routes we calculated in route-optimization.py
        clus_data (dataframe): The processed dataframe of taxi rides
        center (tuple): The center coordinate for start of all rides
    Returns:
        A tuple including the locations list and the counts list (explained above)
    '''
    locations = []
    counts = []
    for route in routes:
        current_loc = []
        current_count = []
        for i in route:
            if i == 0:
                current_loc.append(center)
                current_count.append(0)
            elif i%2 == 1:
                current_loc.append(clus_data['pickup_latlon'][2+(i//2)])
                current_count.append(clus_data['trips_per_day'][2+(i//2)])
            else:
                current_loc.append(clus_data['dropoff_latlon'][1+(i//2)])
                current_count.append(-clus_data['trips_per_day'][1+(i//2)])
    
        locations.append(current_loc)
        counts.append(current_count)
        
    return (locations, counts)

def generate_map(locations, counts, head, tail, center):
    '''
    This function will generate the folium map on html that will show all routes
    with the corresponding bus stops. 
    Input:
        locations (list): list of locations for each route
        counts (list): count of passengers at every location
        head (string): The initial part of our GET request
        tail (string): The trailing part of our GET request
        center (tuple) : Coordinates of the center point
    Returns:
        It returns the map object that we will save and view on html. 
    '''

    m = folium.Map(center, zoom_start = 13)

    for i, routes in enumerate(locations):
        for j in range(len(routes)-1):
        
            col = lambda: random.randint(0,255)
            color = ('#%02X%02X%02X' % (col(),col(),col()))
        
            folium.Marker(routes[j], 
                          popup = 'Number of Passengers getting picked/dropped: '\
                          +str(counts[i][j])+'\nRoute No.: '+str(i)+ '\nBus Stop No.: '+str(j)).add_to(m)
        
            param = str(routes[j][1]) +"," + str(routes[j][0]) +";"+ str(routes[j+1][1]) +","+ str(routes[j+1][0])
            r = requests.get(head + param + tail)
            rj = r.json()
            if r:
                coordinates = rj['routes'][0]['geometry']['coordinates']
        
                new = []
                for c in coordinates:
                    new.append([c[1], c[0]])
        
                folium.PolyLine(new, color = color, tooltip = 'Route No.: ' + str(i)).add_to(m)  
    return m

if __name__ == '__main__':
    
    data = pd.read_csv('clus_data.csv')
    
    data['pickup_latlon'] = data['pickup_latlon'].apply(lambda x: tuple(map(float, x[1:-1].split(', '))))
    data['dropoff_latlon'] = data['dropoff_latlon'].apply(lambda x: tuple(map(float, x[1:-1].split(', '))))
    
    center = (41.904, -87.662)
    routes = np.load("routes_data.npy", allow_pickle = True).tolist()
    head = "https://api.mapbox.com/directions/v5/mapbox/driving/"
    tail = "?geometries=geojson&access_token=pk.eyJ1IjoicmFoaW1yYXNvb2wiLCJhIjoiY2th\
    MHR0dzNvMDVjeDNlbjJuMWMxdWFqYyJ9.uIpUGacDLzUde2oLXfGUdw"
    
    locations, counts = get_loc(routes, data, center)
    map = generate_map(locations, counts,head, tail, center)
    map.save("m.html")
    print("The map has been generated. View the html file using 'xdg-open m.html'")
    print("\nEach line indicates the optimized route, hover over the line to \
    view the route number")
    print("\nEach location maker is a bus stop on that route where we can pick\
    up passengers in optimized manner. Click on it to display the tooltip \
    the shows the bus stop number, the route number and the number of passengers\
    who are getting in (+ sign) and dropping off (- sign)")
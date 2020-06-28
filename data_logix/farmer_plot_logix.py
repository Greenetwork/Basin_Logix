''' this will be a script that helps the farmer on a plot scale spacially aware model
- 1 we meed to be able to impot the geojson and extract as much data as possible form it
- 2 the geojson will the key to geometry and area
- 3 other input params will have to be added by user'''


import geopandas as gpd
import pandas as pd
import os
import xarray as xr
from pathlib import Path, PureWindowsPath, PurePosixPath
import numpy as np
from shapely.geometry import Polygon, Point


def create_uniform_points_from_polygon(polygon_geodataframe, width:int, length:int):
    xmin, ymin, xmax, ymax = polygon_geodataframe.total_bounds #extract geom of the polygon
    rows = int(np.ceil((ymax - ymin) / length)) # capture the rows and recreate them w uniform dist
    cols = int(np.ceil((xmax - xmin) / width)) # capture the columns and recreate them w uniform dist
    XleftOrigin = xmin
    XrightOrigin = xmin + width
    YtopOrigin = ymax
    YbottomOrigin = ymax - length
    points = [] #list where the point geoms will be saved
    for i in range(cols):
        Ytop = YtopOrigin
        Ybottom = YbottomOrigin
        for j in range(rows):
            points.append(
                Point([(XleftOrigin, Ytop), (XrightOrigin, Ytop), (XrightOrigin, Ybottom), (XleftOrigin, Ybottom)]))
            Ytop = Ytop - length
            Ybottom = Ybottom - length
        XleftOrigin = XleftOrigin + width
        XrightOrigin = XrightOrigin + width
    point_grid = gpd.GeoDataFrame({'geometry': points})
    return point_grid

def attach

if __name__ == "__main__":

CONDUIT_INPUT_DATA_DIR = Path(
    os.getcwd() + '\data_logix\conduit_input_data')  # should be testing this on linux by printing out the path and seeing if pathlib detects the os / path combo
os.chdir(CONDUIT_INPUT_DATA_DIR)  # no me gusta esto
my_farmer_plot = gpd.read_file("18102019.geojson")
my_farmer_plot = my_farmer_plot.to_crs({'init': 'epsg:2227'}) # need to go back to meters from degrees - changing crs

crop_spacing = create_uniform_points_from_polygon(polygon_geodataframe=my_farmer_plot, width=500, length=500) # CHANGE THE WIDTH AND LEN BASED ON CROP SPACING
# in this exampel we have 36 points across the farm
#  lets create a test farm
time = pd.date_range("2020-01-06", periods=36)

# data variables
# KSAT
# PPT
# ET
fake_ksat_vals = np.random.rand(36, 1) # random shit
fake_ppt_vals = np.random.rand(36, 1) # random shit

geom_of_pts = [[6366847.595, 2168726.944], [6366847.595, 2168226.944], [6366847.595, 2167726.944],
               [6366847.595, 2167226.944], [6366847.595, 2167226.944], [6366847.595, 2167226.944],
               [6366847.595, 2167226.944], [6366847.595, 2167226.944], [6366847.595, 2167226.944],
               [6366847.595, 2167226.944], [6366847.595, 2167226.944], [6366847.595, 2167226.944],
               [6366847.595, 2167226.944], [6366847.595, 2167226.944], [6366847.595, 2167226.944],
               [6366847.595, 2167226.944], [6366847.595, 2167226.944], [6366847.595, 2167226.944],
               [6366847.595, 2167226.944], [6366847.595, 2167226.944], [6366847.595, 2167226.944],
               [6366847.595, 2167226.944], [6366847.595, 2167226.944], [6366847.595, 2167226.944],
               [6366847.595, 2167226.944], [6366847.595, 2167226.944], [6366847.595, 2167226.944],
               [6366847.595, 2167226.944], [6366847.595, 2167226.944], [6366847.595, 2167226.944],
               [6366847.595, 2167226.944], [6366847.595, 2167226.944], [6366847.595, 2167226.944],
               [6366847.595, 2167226.944], [6366847.595, 2167226.944], [6366847.595, 2167226.944]]

x = [[crop_spacing.geometry.x],[crop_spacing.geometry.y]] # x geom # y geom
apn = [my_farmer_plot.apn[0]] * len(crop_spacing) # creating the APN num of each array each time

farm_xarray = xr.Dataset({"fake_ksat_vals": (["x","y","time"], fake_ksat_vals),
                          "fake_ppt_vals": (["x","y","time"], fake_ppt_vals)},
                          coords = {"lat": (["x", "y"], geom_of_pts),
                                    "long": (["x", "y"], geom_of_pts),
                                    "time": time})

space =




import geopandas as gpd
import math
import matplotlib
import numpy as np
import pandas as pd
import shapely
import sys,os
import pathos.multiprocessing as pmult
import csv
import datetime
from tqdm import tqdm
from geopandas import GeoDataFrame, GeoSeries
from shapely.geometry import Polygon, Point
from rtree import index
from collections import deque
import geopandas


# Monkeypatch geo loading progress bar
def new_geo_op(this, other, op):
    """Operation that returns a GeoSeries"""
    if isinstance(other, geopandas.base.GeoPandasBase):
        this = this.geometry
        crs = this.crs
        if crs != other.crs:
            warn('GeoSeries crs mismatch: {0} and {1}'.format(this.crs,
                                                              other.crs))
        this, other = this.align(other.geometry)
        return gpd.GeoSeries([getattr(this_elem, op)(other_elem)
                             for this_elem, other_elem in tqdm( zip(this, other) )],
                             index=this.index, crs=crs)
    else:
        return gpd.GeoSeries([getattr(s, op)(other)
                             for s in tqdm(this.geometry)],
                             index=this.index, crs=this.crs)

geopandas.base._geo_op = new_geo_op
print geopandas.base._geo_op



def get_available_accounts_queue():
    with open('./accounts_and_proxies/accounts.txt') as f:
        lines = f.read().splitlines()
        username_password = map(lambda x: x.split(":"), lines)
        return deque([{"username": up[0], "password": up[1]}
                      for up in tqdm(username_password)] )

def get_available_proxies_queue():
    with open('./accounts_and_proxies/proxies.txt') as f:
        lines = f.read().splitlines()
        return deque([p for p in tqdm(lines)])


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

# Function transforming initial grid into hex grid layout
def create_point(data):
    i = data[0]
    j = data[1]
    r = data[2]
    xoffset = 0
    yoffset = 0
    p = Point(i*r*math.sqrt(3) + (r*math.sqrt(3)/2 if j%2 else 0) + xoffset,
              j*r*3/4*2+yoffset)
    return shapely.affinity.scale(p.buffer(7).simplify(0.8, preserve_topology=False), xfact=r/7, yfact=r/7)


def create_circle_covering(r, width, height):
    # Accumulate initial circle grid
    circles = []
    for i in range(0, width):
        for j in range(0, height):
            circles.append([i,j,r])

    # Parallel conversion
    pool = pmult.Pool(pmult.cpu_count()-1)
    return pool.map(create_point, circles)


def transform_and_normalize_circle_covering(circles, crs):
    # EPSG:2768 ca meters
    search_circles = GeoDataFrame(geometry=GeoSeries(circles))
    df = GeoDataFrame(geometry=GeoSeries(circles))
    df.crs = {'init': 'epsg:2768'}

    # Transfrom from meter baed EPSG:2768 coordinate system to lat/long degrees based EPSG:4269
    df = df.to_crs(crs)

    # Translation normalization from offset due to change in coordinate systems
    df = df.translate(xoff=141.22, yoff=-30.02, zoff=0.0)
    return df


def translate_circle_covering(covering, lng, lat):
    # Translate circle covering to target location
    #%time df = df.translate(xoff=-122.2, yoff=37.4, zoff=0.0)
    df = covering.translate(xoff=lng, yoff=lat, zoff=0.0)
    return df


def filter_objects_not_inside_geo(geo, data):
    if geo.intersects(data):
        return data
    else:
        return False


def chunked_rect_covering(rect_start_lng, rect_start_lat, rect_width, grid_width, grid_height, target_geometry):
    rects = []
    for i in range(0,grid_width):
        rect_lng = rect_start_lng + rect_width * i
        for j in range(0,grid_height):
            rect_lat = rect_start_lat + rect_width * j
            rects.append(Polygon([(rect_lng, rect_lat),
                                  (rect_lng+rect_width,rect_lat),
                                  (rect_lng+rect_width,rect_lat+rect_width),
                                  (rect_lng,rect_lat+rect_width)]))

    search_rects = GeoDataFrame(geometry=GeoSeries(rects))
    target_geometry_union = target_geometry.buffer(0).geometry.unary_union

    final_rects = []
    pool = pmult.Pool(pmult.cpu_count()-1)
    final_rects = filter(lambda x: x != False and not x.is_empty, pool.map(lambda x: filter_objects_not_inside_geo(target_geometry_union, x), search_rects.geometry))

    return GeoDataFrame(geometry=GeoSeries(final_rects))


def get_scan_region_covering(idx, region_set, target_geometry, norm_circle_covering):
    bounded_rect = target_geometry.intersection(region_set)
    bounded_rect_union = bounded_rect.geometry.unary_union
    b = bounded_rect_union.bounds
    final_circles = []
    if len(b) > 0:
        trans_covering = translate_circle_covering(norm_circle_covering, b[0], b[1])
        pool = pmult.Pool(pmult.cpu_count()-1)
        final_circles = []
        for y in tqdm(pool.imap_unordered(lambda x: filter_objects_not_inside_geo(bounded_rect_union, x), trans_covering.geometry), position=1):
            if y != False:
                final_circles.append(y)
    search_circles = GeoDataFrame(geometry=GeoSeries(final_circles))
    return search_circles

# Generate query plan list
def populate_scan_environment():
    print "[-] Loading accounts"
    accounts = get_available_accounts_queue()
    print "[-] Loading accounts - Done"

    print "[-] Loading proxies"
    proxies = get_available_proxies_queue()
    print "[-] Loading proxies - Done"

    print "[-] Loading shapefiles"
    cities = GeoDataFrame.from_file('./geodata/ca_cities/Cities2015.shp')
    water = GeoDataFrame.from_file('./geodata/baywater/bayarea_allwater.shp')
    print "[-] Loading shapefiles - Done"

    print "[-] Reduce + crop geometry"
    #lng1 = -122.6
    #lat1 = 37.1
    #lng2 = -121.5
    #lat2 = 38




    lng1 = -122.457
    lat1 = 37.76
    lng2 = -122.402
    lat2 = 37.80



    # This is the cropped area - full geojson is all cities in CA
    bayarea = Polygon([(lng1, lat1), (lng2,lat1), (lng2,lat2), (lng1,lat1)])
    bayarea_crop_frame = GeoDataFrame(geometry=GeoSeries([bayarea]),crs={'init': 'epsg:4269'})
    ca_cities = cities.intersection(bayarea_crop_frame.geometry.unary_union)
    ca_water = water.to_crs(ca_cities.crs)
    ca_cities_clean = ca_cities.difference(ca_water.geometry.unary_union)
    simple_ca = GeoDataFrame(geometry=GeoSeries(ca_cities_clean.buffer(0).geometry.unary_union),
                             crs={'init': 'epsg:4269'})

    # Buffer and reduce complexity of geometry - covers costal regions + improves performance
    simple_ca = simple_ca.buffer(0.003).simplify(0.01, preserve_topology=True)
    print "[-] Reduce + crop geometry - Done"

    print "[-] Generate coverings"
    circle_covering = create_circle_covering(70, 50, 65)
    norm_covering = transform_and_normalize_circle_covering(circle_covering, ca_cities_clean.crs)
    search_rects = chunked_rect_covering(lng1 - 0.05, lat1 - 0.1, 0.038, 30, 30, (simple_ca))
    print "[-] Generate coverings - Done"

    with open('scanallocation.csv', 'wb') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        allocated_proxy_count = 0
        allocated_region_count = 0
        allocated_account_count = 0

        for i, r in enumerate(tqdm(search_rects.geometry, position=0)):
            regions = get_scan_region_covering(i, r, ca_cities_clean, norm_covering).geometry
            # one ip for every 40*45 scans (40 workers per ip)
            for region_group_ips in chunks(regions, 40 * 45):
                try:
                    allocated_proxy_count += 1
                    proxy = proxies.pop()
                except Exception as e:
                    print "\nRan out of --- proxies --- {} regions, using {} accounts, using {} proxies".\
                        format(allocated_region_count, allocated_account_count, allocated_proxy_count)
                    return
                # one worker for every 45 scans

                for i, region_group in enumerate(chunks(region_group_ips, 45)):
                    try:
                        allocated_account_count += 1
                        allocate_account = accounts.pop()
                    except Exception as e:
                        print "\nRan out of --- accounts --- {} regions, using {} accounts, using {} proxies".\
                            format(allocated_region_count, allocated_account_count, allocated_proxy_count)
                        return
                    allocated_region_count += 1
                    for region in region_group:
                        # proxy, username, password, region_lat, region_lng
                        spamwriter.writerow(["https://"+proxy, allocate_account["username"], allocate_account["password"], region.centroid.x, region.centroid.y])
        print "\nDone!: {} regions, using {} accounts, using {} proxies".\
            format(allocated_region_count, allocated_account_count, allocated_proxy_count)



# Generate proxy test list
def populate_scan_environment_test_proxies():

    # Recommend using a list of only one or two accounts to test them all
    # until it's confirmed that multiple ips per account v. time may result in bans
    with open('./accounts_and_proxies/accounts.txt') as f:
        lines = f.read().splitlines()
        username_password = map(lambda x: x.split(":"), lines)
        accounts = [{"username": up[0], "password": up[1]}
                      for up in lines]

    with open('./accounts_and_proxies/proxies.txt') as f:
        lines = f.read().splitlines()
        proxies = [p for p in lines]

    lng = -122.457
    lat = 37.76

    with open('scanallocation.csv', 'wb') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        allocated_proxy_count = 0
        allocated_region_count = 0
        allocated_account_count = 0

        for proxy in proxies:
            for account in accounts:
                spamwriter.writerow([
                    "https://"+proxy,
                    account["username"],
                    accont["password"],
                    lng,
                    lat])

        print "\nDone!: {} regions, using {} accounts, using {} proxies".\
            format(allocated_region_count, allocated_account_count, allocated_proxy_count)


if __name__ == "__main__":
    populate_scan_environment()

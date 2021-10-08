#!/usr/bin/env python
# coding: utf-8

# # Data Creation for BLX DB
# #### October 2021
# 
# Combine crop data with parcel data to apply APNS to crop polygons
# 
# Steps in QGIS to be translated into python:
# 
# ~~Load CA_PARCELS_STATEWIDE + i15_Crop_Mapping_2018~~
# ~~ensure both pieces are in the same CRS, I re-projected them to EPSG:3310~~
# ~~Filter CA_PARCELS_STATEWIDE using "County" = 'Kern' → save output as new .shp using \<county>_apns.shp ex: kern_apns.shp~~
# ~~Filter i15_Crop_Mapping_2018 using "COUNTY" = 'Kern'→ save output as new .shp using \<county>_crop.shp ex: kern_crop.shp~~
# ~~Join attributes by location with: kern_crop.shp as the Base Layer, kern_apns.shp as the Join Layer, Geometric predicate: 0 — intersects, Join type: 2 — Take attributes of the feature with largest overlap only (one-to-one~~
# 
# ~~Need to map CLASS2 + SUBCLASS2 to more explicit/human readable data using metadata~~

# In[1]:


import fiona
import os
import geopandas as gpd
import pandas as pd


# In[2]:


get_ipython().system('pwd')


# In[3]:


fiona.listlayers('./raw_data/i15_crop_mapping_2018_gdb/i15_crop_mapping_2018.gdb')


# In[4]:


fiona.listlayers('./raw_data/Parcels_CA_2014.gdb/')


# In[5]:


fiona.listlayers('./raw_data/ca-county-boundaries/CA_Counties/CA_Counties_TIGER2016.dbf')


# ## Load in county data to use a mask to chunk other data by individual county. 

# In[6]:


gdf_counties = gpd.read_file('./raw_data/ca-county-boundaries/CA_Counties/CA_Counties_TIGER2016.dbf',
                    driver='FileGDB',
                    layer='CA_Counties_TIGER2016')
gdf_counties = gdf_counties.to_crs(3310)


# In[7]:


gdf_kern_county = gdf_counties.loc[gdf_counties['NAME']=='Kern']


# In[8]:


# gdf_kern_county.crs
# gdf_kern_county.plot()


# ## Load in crop data

# In[9]:


gdf_crop = gpd.read_file('./raw_data/i15_crop_mapping_2018_gdb/i15_crop_mapping_2018.gdb',
                    driver='FileGDB',
                    layer='i15_Crop_Mapping_2018',
                    mask = gdf_kern_county)
gdf_crop = gdf_crop.to_crs(3310)


# In[10]:


# gdf_crop.crs
# gdf_crop.plot()
# gdf_crop.columns


# ## Load in parcel data

# In[11]:


gdf_apn = gpd.read_file('./raw_data/Parcels_CA_2014.gdb/',
                        driver='FileGDB',
                        layer='CA_PARCELS_STATEWIDE',
                        mask = gdf_kern_county)
gdf_apn = gdf_apn.to_crs(3310)


# In[12]:


# gdf_apn.crs
# gdf_apn.plot()
# gdf_crop.sindex.valid_query_predicates
# gdf_apn.sindex.valid_query_predicates
# gdf_apn.columns


# ### __gdf_combo__
# (spatial intersectional join of __gdf_crop__ with __gdf_apn__)  

# In[13]:


gdf_combo = gpd.sjoin(gdf_crop, gdf_apn, how = 'inner', op = 'intersects')


# In[14]:


# gdf_combo.columns
# gdf_combo.loc[gdf_combo['UniqueID'] == '1509614'].T#.plot() # test crop row


# ### __gdf_over_max__ 
# (selection of the maximum spatial overlap of __gdf_crop__ with __gdf_apn__,   
# practically translates to each __gdf_crop__ row (`uniqueID` is good identifier) being associated with the __gdf_apn__ row (`PARNO` is good identifier) that has the maximum spatial overlap)

# In[15]:


gdf_over = gpd.overlay(gdf_crop, gdf_apn, how = 'intersection')
gdf_over['area_overlap'] = gdf_over.geometry.area
gdf_over_max = gdf_over.loc[gdf_over.groupby('UniqueID')['area_overlap'].agg(pd.Series.idxmax)][['UniqueID','PARNO','area_overlap']]


# ### Merging
# __gdf_combo__ with __gdf_over_max__ 

# In[16]:


gdf_combo_max_area = gdf_combo.merge(gdf_over_max, left_on = ['UniqueID','PARNO'], right_on = ['UniqueID','PARNO'])


# In[17]:


# gdf_combo_max_area.loc[gdf_combo_max_area['UniqueID'] == '1509614'] # test crop row


# In[18]:


gdf_combo_max_area.CROPTYP2.unique()


# ## Add metadata

# In[19]:


meta_data_dict = pd.read_excel('./crop_metadata.xlsx', sheet_name='formatted',header=None, names =['key', 'value']).set_index('key').T.to_dict('records')[0]


# In[20]:


meta_data_dict['G']


# In[21]:


gdf_combo_max_area['crop2018'] = gdf_combo_max_area['CROPTYP2'].map(meta_data_dict)


# In[22]:


gdf_combo_max_area.head().T


# In[ ]:





# In[ ]:





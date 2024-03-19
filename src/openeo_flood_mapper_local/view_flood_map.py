import rioxarray
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.io.img_tiles as cimgt
import numpy as np


def view_flood_map(df):
    # selecting a subsection of the data and reprojecting
    flood_map = df.rio.reproject(f"EPSG:4326", nodata=np.nan)
    # add open streetmap
    request = cimgt.OSM()
    # initialize figure
    fig = plt.figure(figsize=(13,9))
    axis = plt.axes(projection=ccrs.PlateCarree(), frameon=True)
    axis.add_image(request, 15)
    # add the data
    flood_map = flood_map.plot(
    ax=axis,
    transform=ccrs.PlateCarree(),
    levels=[0, 1, 2],
    colors=["#00000000", "#ff0000"],
    add_colorbar=False
    )
    # legend and title
    cbar = fig.colorbar(flood_map, ax=axis, location="bottom", shrink=0.6)
    cbar.ax.get_xaxis().set_ticks([])
    for j, lab in enumerate(['non-flood','flood']):
        cbar.ax.text((2 * j + 1) / 2.0, 0.5, lab, ha='center', va='center')
    cbar.ax.get_xaxis().labelpad = 10
    tk = fig.gca()
    tk = tk.set_title("Flood map")
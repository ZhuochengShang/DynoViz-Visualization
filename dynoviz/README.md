## DynoViz

DynoViz is a tool designed to generate a pyramid structure of raster tile maps using GeoTIFF files, facilitating visualization via OpenLayers. This project efficiently handles large datasets, enabling the creation of tiles at various zoom levels, from a global map (top of the pyramid) containing extensive data to more detailed, zoomed-in views. It utilizes Beast libraries, specifically RDPro and Raptor, for working with raster files.

This command creates PNG tiles in the XYZ format from the designated data source directory. It processes various zoom levels according to user input and stops the operation based on a specified density value.
After starting the Beast server, it enables the on-the-fly generator in the web browser.

#### Usage:
```
beast mplotrasteronce <InputDirToDataSource> <Local file OutputTileDir> zoom:1 density:0 format:PNG
```

### Build Index in Parallel Command

```
beast buildrasterindexp <pathToRasterDir>
```

### Sever
```
beast server
```







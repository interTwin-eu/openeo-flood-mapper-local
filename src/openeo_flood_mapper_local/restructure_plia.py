import argparse
import re
from pathlib import Path

import rioxarray
import xarray as xr
from eotransform_pandas.filesystem.gather import gather_files
from eotransform_pandas.filesystem.naming.geopathfinder_conventions import yeoda_naming_convention

DATA_VERSION = 'V01R03'


def restructure_plia(root: Path, out: Path, tile_long_name: str, tag: str) -> None:
    grid, tile = tile_long_name.split('_')
    grid = f'EQUI7_{grid}'
    plia_df = gather_files(root, yeoda_naming_convention, [re.compile('PLIA-TAG'),
                                                             re.compile(DATA_VERSION),
                                                             re.compile(grid),
                                                             re.compile(tile)], index='extra_field')
    out_dir = out / DATA_VERSION / grid / tile
    out_dir.mkdir(parents=True, exist_ok=True)
    plia_df = plia_df.filter(like=tag, axis=0)
    for _, row in plia_df.iterrows():
        file = row['filepath']
        da = rioxarray.open_rasterio(file, mask_and_scale=True, chunks=500).squeeze().drop_vars("band")
        if row['var_name'] == "PLIA-TAG-NOBS":
            da.encoding.update({'_FillValue': -9999, 'dtype': 'int16', 'zlib': True})
        else:
            da = da / 100
            da.encoding.update({'scale_factor': 0.01, '_FillValue': -9999, 'dtype': 'int16', 'zlib': True})
        da.to_dataset(name='PLIA').to_netcdf(out_dir / f"{file.stem}.nc", mode='w')

def main():
    parser = argparse.ArgumentParser(description="Restructure plia tiffs to be loaded as local openEO dataset.")
    parser.add_argument('root', type=Path, help="root path to the yeoda file structure")
    parser.add_argument('out', type=Path, help="root path to output structure")
    parser.add_argument('tile', type=str, help='long name of tile to process, i.e. "EU020M_E051N015T3"')
    parser.add_argument('tag', type=str, help='tag of ... to process, i.e. "D080"')
    args = parser.parse_args()

    restructure_plia(args.root, args.out, args.tile, args.tag)


if __name__ == '__main__':
    main()

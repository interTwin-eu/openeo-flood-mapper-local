import argparse
import re
from pathlib import Path

import rioxarray
import xarray as xr
from eotransform_pandas.filesystem.gather import gather_files
from eotransform_pandas.filesystem.naming.geopathfinder_conventions import yeoda_naming_convention

DATA_VERSION = 'V1M1R1'


def restructure_sig0(root: Path, out: Path, tile_long_name: str, eventtime: str) -> None:
    grid, tile = tile_long_name.split('_')
    grid = f'EQUI7_{grid}'
    sig0_df = gather_files(root, yeoda_naming_convention, [re.compile('SIG0'),
                                                             re.compile(DATA_VERSION),
                                                             re.compile(grid),
                                                             re.compile(tile)], index='datetime_1')
    out_dir = out / DATA_VERSION / grid / tile
    out_dir.mkdir(parents=True, exist_ok=True)
    sig0_df = sig0_df[sig0_df.index == eventtime]
    sig0_df = sig0_df[sig0_df["band"]=="VV"]

    for _, row in sig0_df.iterrows():
        file = row['filepath']
        da = rioxarray.open_rasterio(file, mask_and_scale=True, chunks=500).squeeze().drop_vars("band")
        da.encoding.update({'scale_factor': 0.1, '_FillValue': -9999, 'dtype': 'int16', 'zlib': True})
        da.to_dataset(name='SIG0').to_netcdf(out_dir / f"{file.stem}.nc", mode='w')

def main():
    parser = argparse.ArgumentParser(description="Restructure sig0 tiffs to be loaded as local openEO dataset.")
    parser.add_argument('root', type=Path, help="root path to the yeoda file structure")
    parser.add_argument('out', type=Path, help="root path to output structure")
    parser.add_argument('tile', type=str, help='long name of tile to process, i.e. "EU020M_E051N015T3"')
    parser.add_argument('eventtime', type=str, help='datetime of the flood event, i.e. "2018-02-28 04:39:08"')
    args = parser.parse_args()

    restructure_sig0(args.root, args.out, args.tile, args.eventtime)

if __name__ == '__main__':
    main()

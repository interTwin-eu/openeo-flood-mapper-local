import argparse
import re
from pathlib import Path

import xarray as xr
from eotransform_pandas.filesystem.gather import gather_files
from eotransform_pandas.filesystem.naming.geopathfinder_conventions import yeoda_naming_convention

DATA_VERSION = 'V0M2R3'


def restructure_hparam(root: Path, out: Path, tile_long_name: str, tag: str) -> None:
    grid, tile = tile_long_name.split('_')
    grid = f'EQUI7_{grid}'
    hparam_df = gather_files(root, yeoda_naming_convention, [re.compile('SIG0-HPAR'),
                                                             re.compile(DATA_VERSION),
                                                             re.compile(grid),
                                                             re.compile(tile)], index='extra_field')
    out_dir = out / DATA_VERSION / grid / tile
    out_dir.mkdir(parents=True, exist_ok=True)
    hparam_df = hparam_df.filter(like=tag, axis=0)
    for orbit, grouped_df in hparam_df.groupby('extra_field'):
        da = xr.open_mfdataset(grouped_df['filepath'], concat_dim='band', combine='nested', chunks=500,
                               mask_and_scale=True)['band_data']
        var_names = [v.split('-')[-1] for v in grouped_df['var_name'].to_list()]
        da = da.assign_coords({'band': ('band', var_names)})
        ds = da.to_dataset(dim='band')
        encoding = {dv: {'scale_factor': 0.1, '_FillValue': -9999, 'dtype': 'int16', 'zlib': True}
                    for dv in ds.data_vars if dv != 'NOBS'}
        encoding['NOBS'] = {'_FillValue': -9999, 'dtype': 'int16', 'zlib': True}
        ds.to_netcdf(out_dir / f"{orbit}.nc", mode='w', encoding=encoding)

def main():
    parser = argparse.ArgumentParser(description="Restructure hparam tiffs to be loaded as local openEO dataset.")
    parser.add_argument('root', type=Path, help="root path to the yeoda file structure")
    parser.add_argument('out', type=Path, help="root path to output structure")
    parser.add_argument('tile', type=str, help='long name of tile to process, i.e. "EU020M_E051N015T3"')
    parser.add_argument('tag', type=str, help='tag of ... to process, i.e. "D080"')
    args = parser.parse_args()

    restructure_hparam(args.root, args.out, args.tile, args.tag)


if __name__ == '__main__':
    main()

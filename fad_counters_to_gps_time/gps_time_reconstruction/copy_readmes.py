import pkg_resources
import shutil


def copy_top_level_readme_to(path):
    readme_res_path = pkg_resources.resource_filename(
        'fad_counters_to_gps_time',
        'gps_time_reconstruction/resources/top_level_readme.md'
    )
    shutil.copy(readme_res_path, path)

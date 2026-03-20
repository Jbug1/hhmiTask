import os

import dagster as dg

from dagster_pipeline.assets import (
    openMicroscopy_data,
    epfl_data,
    empiar_data,
    janelia_data,
    hemibrain_data,
    combined_metadata,
)

from dagster_pipeline.resources import PipelineConfig

CONFIGS = os.path.join(os.path.dirname(__file__), "configs")

defs = dg.Definitions(
    assets=[openMicroscopy_data, epfl_data, empiar_data, janelia_data, hemibrain_data, combined_metadata],
    resources={
        "pipeline_config": PipelineConfig(
            output_dir="/Users/jonahpoczobutt/projects/hhmi_res",
            openMicroscopy_config_path=f"{CONFIGS}/openMicroscopyConfig.py",
            epfl_config_path=f"{CONFIGS}/epflConfig.py",
            empiar_config_path=f"{CONFIGS}/empiarConfig.py",
            janelia_config_path=f"{CONFIGS}/janeliaConfig.py",
            hemibrain_config_path=f"{CONFIGS}/hemibrainConfig.py",
        ),
    },
)

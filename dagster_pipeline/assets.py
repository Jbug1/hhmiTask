import os
import shutil
import dagster as dg
import pandas as pd

from dagster_pipeline.resources import PipelineConfig, load_config
from retrievers import openMicroscopy, empiar, epfl, janelia, hemibrain

SOURCES = ["openMicroscopy", "empiar", "epfl", "janelia", "hemibrain"]

def _asset_dir(pipeline_config: PipelineConfig, asset_name: str) -> str:
    """Return the subdirectory for a given asset."""
    return os.path.join(pipeline_config.output_dir, asset_name)


def _archive_config(context, pipeline_config: PipelineConfig, config_path: str, asset_name: str):
    """Copy the config file to the asset's output directory."""
    out_dir = os.path.join(_asset_dir(pipeline_config, asset_name), 'config_log')
    shutil.copy(config_path, out_dir)
    context.log.info(f"Config archived to {out_dir}")


@dg.asset
def openMicroscopy_data(context: dg.AssetExecutionContext, pipeline_config: PipelineConfig):

    config = load_config(pipeline_config.openMicroscopy_config_path)
    output_path = os.path.join(pipeline_config.output_dir, "openMicroscopy")

    r = openMicroscopy(
        sample_ids=config.sample_ids,
        output_path=output_path,
        jobs=config.jobs,
    )
    r.create_directory_structure()

    _archive_config(context, pipeline_config, pipeline_config.openMicroscopy_config_path, "openMicroscopy")

    r.retrieve_metadata_and_data()


@dg.asset
def empiar_data(context: dg.AssetExecutionContext, pipeline_config: PipelineConfig):

    config = load_config(pipeline_config.empiar_config_path)
    output_path = os.path.join(pipeline_config.output_dir, "empiar")

    r = empiar(
        sample_ids=config.sample_ids,
        output_path=output_path,
        jobs=config.jobs,
    )
    r.create_directory_structure()

    _archive_config(context, pipeline_config, pipeline_config.empiar_config_path, "empiar")

    r.retrieve_metadata_and_data()


@dg.asset
def epfl_data(context: dg.AssetExecutionContext, pipeline_config: PipelineConfig):
    config = load_config(pipeline_config.epfl_config_path)
    output_path = os.path.join(pipeline_config.output_dir, "epfl")

    r = epfl(
        samples=config.samples,
        output_path=output_path,
        jobs=config.jobs,
    )
    r.create_directory_structure()

    _archive_config(context, pipeline_config, pipeline_config.epfl_config_path, "epfl")

    r.retrieve_metadata_and_data()

@dg.asset
def janelia_data(context: dg.AssetExecutionContext, pipeline_config: PipelineConfig):
    config = load_config(pipeline_config.janelia_config_path)
    output_path = os.path.join(pipeline_config.output_dir, "janelia")



    r = janelia(
        sample_ids=config.sample_ids,
        output_path=output_path,
        jobs=config.jobs,
    )
    r.create_directory_structure()

    _archive_config(context, pipeline_config, pipeline_config.janelia_config_path, "janelia")

    r.retrieve_metadata_and_data()


@dg.asset
def hemibrain_data(context: dg.AssetExecutionContext, pipeline_config: PipelineConfig):
    config = load_config(pipeline_config.hemibrain_config_path)
    output_path = os.path.join(pipeline_config.output_dir, "hemibrain")

    r = hemibrain(
        sample_ids=config.sample_ids,
        output_path=output_path,
        jobs=config.jobs,
    )
    r.create_directory_structure()

    _archive_config(context, pipeline_config, pipeline_config.hemibrain_config_path, "hemibrain")

    r.retrieve_metadata_and_data()


@dg.asset(deps=[openMicroscopy_data, empiar_data, epfl_data, janelia_data, hemibrain_data])
def combined_metadata(context: dg.AssetExecutionContext, pipeline_config: PipelineConfig):

    frames = []
    for source in SOURCES:
        csv_path = os.path.join(pipeline_config.output_dir, source, "metadata", "metadata.csv")
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            df["source"] = source
            frames.append(df)
            context.log.info(f"loaded {len(df)} rows from {source}")
        else:
            context.log.warning(f"no metadata.csv found for {source}")

    combined = pd.concat(frames, ignore_index=True)
    os.makedirs(os.path.join(pipeline_config.output_dir, 'combined_metadata'), exist_ok=True)
    out_path = os.path.join(pipeline_config.output_dir, "combined_metadata", "combined_metadata.csv")
    combined.to_csv(out_path, index=False)
    context.log.info(f"wrote {len(combined)} rows to {out_path}")
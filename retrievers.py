#classes for grabbing data from various sources
import requests
import logging
from typing import List
from os import makedirs, path
from joblib import Parallel, delayed
from omero.gateway import BlitzGateway
import pandas as pd
import numpy as np
import re
import s3fs
import zarr
from cloudvolume import CloudVolume


class retriever:

    def __init__(self,
                 jobs: int,
                 output_path: str,
                 retries: int = 10):

        self.jobs = jobs
        self.retries = retries
        self.output_path = output_path
        self.logger = self.set_logger()

    def set_logger(self):
        log_dir = path.join(self.output_path, 'config_log')
        makedirs(log_dir, exist_ok=True)

        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)

        logger.propagate = False

        if not logger.handlers:
            fh = logging.FileHandler(path.join(log_dir, f'{self.__class__.__name__}.log'))
            fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            logger.addHandler(fh)

        return logger

    def create_directory_structure(self):
        #creation of output directory structure is part of initialization
        makedirs(self.output_path, exist_ok = True)
        makedirs(f'{self.output_path}/data', exist_ok = True)
        makedirs(f'{self.output_path}/metadata', exist_ok = True)
        makedirs(f'{self.output_path}/config_log', exist_ok = True)

    def stream_retry(self, path, download_path):
        """
        streaming download of image with built-in retries
        """

        last_error = None
        for _ in range(self.retries):

            try:
                #if we are downloading an image, stream and write chunks
                response = requests.get(path, stream=True)

                #still need to make sure
                if response.status_code == 200:
                    with open(download_path, "wb") as handle:

                        for chunk in response.iter_content(chunk_size=8192):
                            handle.write(chunk)

                    return 'complete'

            except Exception as e:
                last_error = e

        if last_error is not None:
            return str(last_error)

        else:
            return response.status_code

    def request_retry(self, path, json = True):
        """ 
        default to retry n times at api endpoint
        returns error (if necessary) and response (if any)
        """
        e = None
        for _ in range(self.retries):

            try:

                response = requests.get(path)

                if response.status_code == 200:

                    if json:
                        return response.status_code, response.json()
                    
                    else:
                        return response.status_code, response.text
            
            except Exception as e:
                if _ == self.retries - 1:
                    return str(e), None
        
        else:
            return response.status_code, None

class openMicroscopy(retriever):

    def __init__(self,
                 sample_ids: List[int] = [],
                 output_path: str = '',
                 jobs: int = 1):
        
        super().__init__(output_path=output_path,
                         jobs = jobs)
        
        self.sample_ids = sample_ids
        self.sample_metadata_path = 'https://idr.openmicroscopy.org/api/v0/m/datasets/sample/images/'
        self.image_metadata_path = 'https://idr.openmicroscopy.org/webgateway/imgData/sample/'

    def retrieve_metadata_and_data(self):
        """
        gathers data and metadata, assigning to its proper directory
        """

        for sample_id in self.sample_ids:

            try:

                self.logger.info(f'retrieving sample {sample_id}')

                metadata_response_code, metadata_raw = self.request_retry(self.sample_metadata_path.replace('sample', str(sample_id)))

                if metadata_response_code != 200:
                    raise ConnectionError(f'initial metadata response code: {metadata_response_code}')
                

                reformatted_metadata = self.reformat_metadata(metadata_raw['data'])

                #grab and save image datasets for reformatted metadata
                for (image_id, dataset_id, axes) in zip(reformatted_metadata['image_ID'],
                                                        reformatted_metadata['dataset_ID'],
                                                        reformatted_metadata['axes']):

                    self.logger.info(f'downloading image {image_id}')
                    self.download_image(image_id, dataset_id, axes)
                    self.logger.info(f'downloaded image {image_id}')

                #append reformatted_metadata to metadata folder
                reformatted_metadata.iloc[:1, 2:].to_csv(path.join(self.output_path,'metadata/metadata.csv'), mode = 'a', index = False)
                self.logger.info(f'sample {sample_id} complete')

            except Exception as e:
                self.logger.error(f'sample {sample_id} failed: {e}')


    def reformat_metadata(self, metadata_raw):
        """
        reformats for future harmonization with other sources
        """

        image_ids = []
        dataset_ids = []
        axes = []
        image_widths = []
        image_heights = []
        x_resolution = []
        y_resolution = []
        z_resolution = []

        for image in metadata_raw:

            response_code, image_metadata = self.request_retry(self.image_metadata_path.replace('sample', str(image['@id'])))

            if response_code != 200:
                raise ConnectionError(f'could not get response for image: {image["@id"]}')

            image_ids.append(image['@id'])
            dataset_ids.append(image_metadata['meta']['datasetId'])
            axes.append(image_metadata['meta']['imageName'])
            image_widths.append(image_metadata['size']['width'])
            image_heights.append(image_metadata['size']['height'])
            x_resolution.append(image_metadata['pixel_size']['x'])
            y_resolution.append(image_metadata['pixel_size']['y'])
            z_resolution.append(image_metadata['pixel_size']['z'])

        return pd.DataFrame({'image_ID': image_ids,
                             'axes': axes,
                             'dataset_ID': dataset_ids,
                             'constituent_files': [len(metadata_raw)] * len(metadata_raw),
                             'image_width': image_widths,
                             'image_height': image_heights,
                             'x_resolution': x_resolution,
                             'y_resolution': y_resolution,
                             'z_resolution': z_resolution,
                             'origin': [np.nan] * len(metadata_raw)
                             })


    def connect(self):
        """ 
        connect to server for exporting of image files
        """
    
        conn = BlitzGateway('public', 'public', host= 'idr.openmicroscopy.org', secure=True)
        conn.connect()
        conn.c.enableKeepAlive(60)
        return conn
    
    def download_image(self, image_id, dataset_ID, axes):
        """  
        downloads the actual image (one set of two axes) and saves to output directory

        each image download attempt will get and close its own connection
        """

        #make sure output directory exists to receive the image
        makedirs(path.join(self.output_path, 'data', str(dataset_ID)), exist_ok = True)

        #create connection
        conn = self.connect()
   
        outpath = path.join(self.output_path, 'data', str(dataset_ID), f'{axes}.tiff')
        
        exporter = conn.createExporter()
        exporter.addImage(image_id)
        size = exporter.generateTiff()
        
        with open(outpath, 'wb') as handle:

            offset = 0
            while offset < size:

                #use max allowable size from idr
                chunk = exporter.read(offset, min(1048576, size - offset))
                handle.write(chunk)
                offset += len(chunk)
        
        exporter.close()
        conn.close()

class empiar(retriever):

    def __init__(self,
                 sample_ids: List[int] = [],
                 output_path: str = '',
                 jobs: int = 1):
        
        super().__init__(output_path=output_path,
                         jobs = jobs)
        
        self.sample_ids = sample_ids
        self.sample_metadata_path = 'https://www.ebi.ac.uk/empiar/api/entry/EMPIAR-sample'
        self.image_data_path = 'https://ftp.ebi.ac.uk/empiar/world_availability/sample/data/'

    def retrieve_metadata_and_data(self):
        """
        gathers data and metadata, assigning to its proper directory
        """

        for sample_id in self.sample_ids:

            try:

                self.logger.info(f'retrieving sample {sample_id}')

                metadata_response_code, metadata_raw = self.request_retry(self.sample_metadata_path.replace('sample', str(sample_id)))

                if metadata_response_code != 200:
                    raise ConnectionError(f'initial metadata response code: {metadata_response_code}')

                reformatted_metadata = self.reformat_metadata(metadata_raw, sample_id)

                self.download_images(sample_id)

                #append reformatted_metadata to metadata folder
                reformatted_metadata.to_csv(path.join(self.output_path,'metadata/metadata.csv'), mode = 'a', index = False)
                self.logger.info(f'sample {sample_id} complete')

            except Exception as e:
                self.logger.error(f'sample {sample_id} failed: {e}')

    def download_images(self, sample_id):
        """
        lists directory and downloads all files for a given sample from the EMPIAR HTTPS server
        """

        out_dir = path.join(self.output_path, 'data', str(sample_id))
        makedirs(out_dir, exist_ok=True)

        status_code, response = self.request_retry(self.image_data_path.replace('sample',str(sample_id)), False)

        if status_code != 200:
            raise ConnectionError(f'could not list files for {sample_id}: {status_code}')

        filenames = [i for i in re.findall(r'href="([^"?/][^"]*\.[^"]+)"', response) if 'slice' in i.lower()]

        self.logger.info(f'found {len(filenames)} files for sample {sample_id}')

        Parallel(n_jobs=self.jobs, backend='threading')(
            delayed(self.download_one)(sample_id, f) for f in filenames
        )

    def download_one(self, sample_id, filename):
        url = self.image_data_path.replace('sample', str(sample_id)) + filename
        dl_path = path.join(self.output_path, 'data', str(sample_id), str(filename))
        result = self.stream_retry(url, dl_path)
        self.logger.info(f'{filename}: {result}')

    def reformat_metadata(self, metadata_raw, sample_id):

        entry = list(metadata_raw.values())[0]['imagesets'][0]

        return pd.DataFrame({
            'dataset_ID': [sample_id],
            'constituent_files': [entry['num_images_or_tilt_series']],
            'image_width': [entry['image_width']],
            'image_height': [entry['image_height']],
            'image_z': [np.nan],
            'x_resolution': [entry['pixel_width']/10],
            'y_resolution': [entry['pixel_height']/10],
            'z_resolution' : [np.nan],
            'origin': [np.nan]
        })

class epfl(retriever):

    def __init__(self,
                 samples: List[str] = [],
                 output_path: str = '',
                 jobs: int = 1):
        
        super().__init__(output_path=output_path,
                         jobs = jobs)
        
        self.samples = samples
        self.image_data_path = "https://documents.epfl.ch/groups/c/cv/cvlab-unit/www/data/%20ElectronMicroscopy_Hippocampus/training_groundtruth.tif"


    def retrieve_metadata_and_data(self):
        """
        gathers data and metadata, assigning to its proper directory
        """

        results = Parallel(n_jobs=self.jobs, backend='threading')(
            delayed(self.process_sample)(sample_id) for sample_id in self.samples
        )

        metadata_frames = [r for r in results if r is not None]
        if metadata_frames:
            pd.concat(metadata_frames, ignore_index=True).to_csv(
                path.join(self.output_path, 'metadata/metadata.csv'), mode='a', index=False
            )

    def process_sample(self, sample_id):
        """
        retrieves data and metadata for a single sample
        """

        try:
            self.logger.info(f'retrieving sample {sample_id}')

            reformatted_metadata = self.reformat_metadata(sample_id=sample_id)
            self.download_images(sample_id)

            self.logger.info(f'sample {sample_id} complete')
            return reformatted_metadata

        except Exception as e:
            self.logger.error(f'sample {sample_id} failed: {e}')
            return None

    def download_images(self, sample_id):
        """
        downloads image file for a given sample from the EPFL HTTPS server
        """

        out_dir = path.join(self.output_path, 'data', str(sample_id))
        makedirs(out_dir, exist_ok=True)

        url = self.image_data_path.replace('sample', str(sample_id))
        out_path = path.join(out_dir,'data.tiff')
        result = self.stream_retry(url, out_path)

        if result != 200:
            self.logger.error(f'{sample_id}: {result}')
        else:
            self.logger.info(f'{sample_id}: {result}')

    def reformat_metadata(self, sample_id):

        return pd.DataFrame({
            'dataset_ID': [sample_id],
            'constituent_files': [4],
            'image_width': [1065],
            'image_height':[2048],
            'image_z': [165],
            'x_resolution': [5],
            'y_resolution': [5],
            'z_resolution' : [5],
            'origin': [np.nan]
        })

class janelia(retriever):

    def __init__(self,
                 sample_ids: List[int] = [],
                 output_path: str = '',
                 jobs: int = 1):
        
        super().__init__(output_path=output_path,
                         jobs = jobs)
        
        self.sample_ids = sample_ids
        self.sample_metadata_path = "https://janelia-cosem-datasets.s3.amazonaws.com/sample/sample.zarr/recon-2/em/fibsem-int16/.zattrs"
        self.sample_image_data_path = "janelia-cosem-datasets/sample/sample.zarr"

    def get_metadata_2(self, sample_id):

        try:
  
            fs = s3fs.S3FileSystem(anon=True)
            store = s3fs.S3Map(root=self.sample_image_data_path.replace('sample',sample_id), s3=fs)
            group = zarr.open(store, mode='r')

            arr = group['recon-2/em/fibsem-int16/s0']
            return (200, arr.shape)
        
        except Exception as e:
            return (e, '')

    def reformat_metadata(self, raw, raw_2, dataset_name):
        """
        reformats OpenOrganelle zarr attrs into standardized DataFrame
        """

        multiscale = raw['multiscales'][0]
        axes = [a['name'] for a in multiscale['axes']]

        s0 = multiscale['datasets'][0]['coordinateTransformations'][0]['scale']
        resolution = dict(zip(axes, s0))

        return pd.DataFrame({
            'dataset_ID': [dataset_name],
            'constituent_files': [min(raw_2[0],20)],
            'image_width': [raw_2[1]],
            'image_height': [raw_2[2]],
            'image_z': [raw_2[0]],
            'x_resolution': [resolution['x']],
            'y_resolution': [resolution['y']],
            'z_resolution': [resolution['z']],
            'origin': [np.nan]
        })

    def retrieve_metadata_and_data(self):
        """
        gathers data and metadata, assigning to its proper directory
        """

        for sample_id in self.sample_ids:

            try:

                self.logger.info(f'retrieving sample {sample_id}')

                metadata_response_code, metadata_raw = self.request_retry(self.sample_metadata_path.replace('sample', sample_id))

                metadata_response_code_2, metadata_raw_2 = self.get_metadata_2(sample_id)

                if metadata_response_code != 200:
                    raise ConnectionError(f'initial metadata response code: {metadata_response_code} for {sample_id}')

                if metadata_response_code_2 != 200:
                    raise ConnectionError(f'2nd metadata response code: {metadata_response_code_2} for {sample_id}')

                reformatted_metadata = self.reformat_metadata(metadata_raw, metadata_raw_2, sample_id)
                self.download_images(sample_id)

                #append reformatted_metadata to metadata folder
                reformatted_metadata.to_csv(path.join(self.output_path,'metadata/metadata.csv'), mode = 'a', index = False)
                self.logger.info(f'sample {sample_id} complete')

            except Exception as e:
                self.logger.error(f'sample {sample_id} failed: {e}')

    def download_images(self, sample_id):

        out_dir = path.join(self.output_path, 'data', str(sample_id))
        makedirs(out_dir, exist_ok=True)

        fs = s3fs.S3FileSystem(anon=True)
        store = s3fs.S3Map(root=self.sample_image_data_path.replace('sample', sample_id), s3=fs)
        group = zarr.open(store, mode='r')

        arr = group['recon-2/em/fibsem-int16/s0']

        for i in range(min(20,len(arr))):

            local = arr[i]
            np.save(path.join(out_dir, f'{i}.npy'), local)

class hemibrain(retriever):

    def __init__(self,
                 sample_ids: List[int] = [],
                 output_path: str = '',
                 jobs: int = 1):
        
        super().__init__(output_path=output_path,
                         jobs = jobs)
        
        self.sample_ids = [str(i) for i in sample_ids]
        self.sample_metadata_path = 'https://storage.googleapis.com/neuroglancer-janelia-flyem-hemibrain/emdata/clahe_yz/jpeg/info'
        

    def reformat_metadata(self, raw, dataset_name, origin):
        scale = raw['scales'][0]
        size = scale['size']
        res = scale['resolution']

        return pd.DataFrame({
            'dataset_ID': [dataset_name],
            'constituent_files': [size[2]],
            'image_width': [size[0]],
            'image_height': [size[1]],
            'image_z': [size[2]],
            'x_resolution': [res[0]],
            'y_resolution': [res[1]],
            'z_resolution': [res[2]],
            'origin': [str(origin)]
        })


    def retrieve_metadata_and_data(self):
        """
        gathers data and metadata, assigning to its proper directory
        """

        for sample_id in self.sample_ids:

            try:

                self.logger.info(f'retrieving sample {sample_id}')

                metadata_response_code, metadata_raw = self.request_retry(self.sample_metadata_path)

                if metadata_response_code != 200:
                    raise ConnectionError(f'initial metadata response code: {metadata_response_code} for {sample_id}')
                
                vol = CloudVolume(
                    'precomputed://gs://neuroglancer-janelia-flyem-hemibrain/emdata/clahe_yz/jpeg',
                    use_https=True, progress=False
                )

                origin = [np.random.randint(0, s - 1000) for s in vol.shape[:3]]

                reformatted_metadata = self.reformat_metadata(metadata_raw, sample_id, origin)

                self.download_images(sample_id, origin)

                #append reformatted_metadata to metadata folder
                reformatted_metadata.to_csv(path.join(self.output_path,'metadata/metadata.csv'), mode = 'a', index = False)
                self.logger.info(f'sample {sample_id} complete')

            except Exception as e:
                self.logger.error(f'sample {sample_id} failed: {e}')

    def download_images(self, sample_id, origin):

        vol = CloudVolume(
            'precomputed://gs://neuroglancer-janelia-flyem-hemibrain/emdata/clahe_yz/jpeg',
            use_https=True, progress=True
        )

        makedirs(path.join(self.output_path,'data',sample_id))

        # Download the block
        data = vol[origin[0]:origin[0]+1000,
                    origin[1]:origin[1]+1000,
                    origin[2]:origin[2]+1000]
        
        np.save(path.join(self.output_path,'data',sample_id,'data.npy'), data)


    






    
        
        

    

FROM osgeo/gdal:ubuntu-full-3.4.0
MAINTAINER Alex Zvoleff azvoleff@conservation.org

RUN apt-get update && \
    apt-get install -yq locales python3-boto3 python3-pip \
        apt-transport-https ca-certificates gnupg git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*  && \
    mkdir -p /work

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
        | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
        apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
    apt-get update -y && apt-get install google-cloud-sdk -y

RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen
ENV LANG en_US.UTF-8  ENV LANGUAGE en_US:en  ENV LC_ALL en_US.UTF-8
ENV RESET_BUILD df

RUN chown $USER:$USER /work

RUN pip3 install 'Numpy==1.19'
RUN git clone https://github.com/monocongo/climate_indices /work/climate_indices
RUN cd /work/climate_indices && \
	python setup.py install

RUN apt-get update -y && apt-get install -y \
    python3-xarray \
    python3-rasterio
RUN pip3 install rioxarray
RUN pip3 install netcdf4
RUN pip3 install rollbar
RUN python -m pip install "dask>=2022.2.1[distributed]" --upgrade
RUN pip3 install bokeh>=2.1.1
RUN pip3 install openpyxl
RUN pip3 install bottleneck --upgrade

RUN apt-get install -y libpq-dev
RUN pip3 install geocube

RUN pip3 install 'google-cloud-storage'

ADD data /data

RUN mkdir ~/.aws && \
    mv /data/aws_credentials ~/.aws/credentials && \
    mv /data/aws_config ~/.aws/config
RUN gcloud auth activate-service-account --key-file=/data/gcs_credentials.json

# Below is needed to ensure keys with dots in them can be handled from S3
RUN echo "[s3]" >> ~/.boto && \
    echo "calling_format = boto.s3.connection.OrdinaryCallingFormat]" >> ~/.boto

ADD entrypoint.sh /work/entrypoint.sh
ADD cropland_match_to_esa.py /work/cropland_match_to_esa.py
ADD esa_cci_transitions.py /work/esa_cci_transitions.py
ADD natural_conversion.py /work/natural_conversion.py
ADD parallel_functions.py /work/parallel_functions.py
ADD ESA_CCI_Natural_Conversion_Coding_v2.xlsx /work/ESA_CCI_Natural_Conversion_Coding_v2.xlsx

# dask dashboard
EXPOSE 8787

WORKDIR /work

ENTRYPOINT ["./entrypoint.sh"]

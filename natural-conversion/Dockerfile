FROM osgeo/gdal:ubuntu-full-3.4.0
MAINTAINER Alex Zvoleff azvoleff@conservation.org

RUN apt-get update && \
    apt-get install -y software-properties-common && \
    rm -rf /var/lib/apt/lists/* && \
    add-apt-repository -r ppa:ubuntugis/ppa && \
    apt-get update

RUN apt-get install -yq locales python3-boto3 python3-pip \
        apt-transport-https ca-certificates gnupg git && \
    apt-get clean && \
    apt-get install python-numpy gdal-bin libgdal-dev libpq-dev -y

RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen
ENV LANG en_US.UTF-8  ENV LANGUAGE en_US:en  ENV LC_ALL en_US.UTF-8
ENV RESET_BUILD df

RUN pip3 install numpy --upgrade
RUN pip3 install rioxarray netcdf4 "dask>=2022.2.1[complete]" bokeh>=2.1.1 openpyxl \
    bottleneck geocube rollbar psutil --upgrade
RUN pip3 install numba

RUN mkdir -p /work && \
    chown $USER:$USER /work

ADD data /data

RUN mkdir ~/.aws && \
    mv /data/aws_credentials ~/.aws/credentials && \
    mv /data/aws_config ~/.aws/config

# Below is needed to ensure keys with dots in them can be handled from S3
RUN echo "[s3]" >> ~/.boto && \
    echo "calling_format = boto.s3.connection.OrdinaryCallingFormat]" >> ~/.boto

ADD entrypoint.sh /work/entrypoint.sh
ADD cropland_match_to_esa.py /work/cropland_match_to_esa.py
ADD esa_cci_transitions.py /work/esa_cci_transitions.py
ADD natural_conversion.py /work/natural_conversion.py
ADD parallel_functions.py /work/parallel_functions.py
ADD ESA_CCI_Natural_Conversion_Coding_v2.xlsx /work/ESA_CCI_Natural_Conversion_Coding_v2.xlsx

#ENV MALLOC_TRIM_THRESHOLD_=0

# dask dashboard
EXPOSE 8787

WORKDIR /work

ENTRYPOINT ["./entrypoint.sh"]

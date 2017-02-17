# The Dev dockerfile creates /var/pipetree_dev/ as a VOLUME
# and automatically installs it via: pip install -e .
# Usage: docker build Docker/Dev -t pipetree-dev
#        docker run -v /path/to/dev/repo/:/var/pipetree_dev/ -t pipetree-dev

FROM mmcdermo/pipetree-base

RUN mkdir /var/pipetree_deps/
COPY ./dev_startup.sh /var/pipetree_deps/
RUN cd /var/pipetree_deps/ && chmod 0700 ./dev_startup.sh

VOLUME /var/pipetree/
VOLUME /var/pipetree_dev/

EXPOSE 8888:8888
ENTRYPOINT /var/pipetree_deps/dev_startup.sh && python3.5 /var/pipetree/main.py
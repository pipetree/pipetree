FROM mmcdermo/pipetree-base

RUN mkdir /var/pipetree_deps/
COPY ./pipetree_setup.sh /var/pipetree_deps/
RUN cd /var/pipetree_deps/ && chmod 0700 ./pipetree_setup.sh && ./pipetree_setup.sh && echo "Hash 9zacbd"

VOLUME /var/pipetree/

EXPOSE 8888:8888
ENTRYPOINT python3.5 /var/pipetree/main.py
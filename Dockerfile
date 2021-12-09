FROM public.ecr.aws/sam/build-nodejs14.x
RUN yum install -y which openssl11-devel openssl11-static \
      && curl -sO https://ftp.postgresql.org/pub/source/v14.1/postgresql-14.1.tar.bz2 \
      && tar -xjf postgresql-14.1.tar.bz2 \
      && cd $LAMBDA_TASK_ROOT/postgresql-14.1 \
      && ./configure --with-openssl --without-readline --prefix=$LAMBDA_TASK_ROOT \
      && cd src/interfaces/libpq && make && make install && cd - \
      && cd src/bin/pg_config && make && make install && cd - \
      && cp src/include/postgres_ext.h src/include/pg_config_ext.h src/include/pg_config.h $LAMBDA_TASK_ROOT/include/ \
      && cp /lib64/libcrypto.so.1.1 /lib64/libssl.so.1.1 $LAMBDA_TASK_ROOT/lib/

ENV PATH $LAMBDA_TASK_ROOT/bin:$PATH
ENV CPATH $LAMBDA_TASK_ROOT/include
COPY package.json package-lock.json ./
RUN npm ci

FROM python:3.5-alpine
RUN mkdir dist
WORKDIR /dist
COPY ./requirements.txt .
COPY ./main_sql.py .
COPY ./mathematician ./mathematician
RUN pip install -r requirements.txt
CMD python main_sql.py ../config/config.json

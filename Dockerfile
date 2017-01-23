FROM python:3.5
ADD ./dist /dist
WORKDIR /dist
RUN pip install -r requirements.txt
CMD python main.py ../config/config.json
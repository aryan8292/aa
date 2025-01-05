FROM python:3.10

WORKDIR /app

COPY . /app/
RUN pip install --upgrade motor
RUN pip install --upgrade umongo
RUN pip install -r requirements.txt

CMD ["python3", "bot.py"]

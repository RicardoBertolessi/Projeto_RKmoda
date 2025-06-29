import schedule
import time

from Transform_rk import executar_transformacao
from Carrega import executar_carga

def job_transform():
    print("Executando transformação...")
    executar_transformacao()

def job_load():
    print("Executando carga no banco...")
    executar_carga()

if __name__ == '__main__':

    schedule.every().day.at("03:00").do(job_transform)
    schedule.every().day.at("03:12").do(job_load)

    print("ETL agendado: transformação às 02:00 e carga às 02:30.")

    while True:
        schedule.run_pending()
        time.sleep(60)

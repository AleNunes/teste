import logging
from sqlalchemy import text
from datetime import datetime
from functools import wraps
import socket
from io import StringIO
from inspect import getfile, stack
import os
from common.utils.db_utils import *  # Supondo que get_engine esteja em utils/database.py



#print(os.getcwd())


# Configuração básica de logging
logging.basicConfig(
    filename='./logs/app.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

def log_execution(func):
    """
    Decorator para registrar logs de execução de funções.
    Registra:
        - Nome da função
        - Argumentos posicionais e nomeados
        - Hora de início e fim
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        logging.info(f"Executando '{func.__name__}' | Args: {args} | Kwargs: {kwargs}")
        
        try:
            result = func(*args, **kwargs)
            logging.info(f"'{func.__name__}' executada com sucesso. Resultado: {result}")
            return result
        except Exception as e:
            logging.error(f"Erro na execução de '{func.__name__}': {e}", exc_info=True)
            raise
        finally:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logging.info(f"'{func.__name__}' finalizada. Duração: {duration:.2f} segundos")
    
    return wrapper




def log_to_db(custom_message=None, log_on_success=True):
    """
    Decorator para registrar logs no banco de dados, incluindo mensagens intermediárias,
    login do usuário e o arquivo que executou a função.
    
    :param custom_message: Mensagem personalizada para o log.
    :param log_on_success: Determina se o log será registrado para execuções bem-sucedidas.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            function_name = func.__name__
            arguments = str({'args': args, 'kwargs': kwargs})
            hostname = socket.gethostname()
            ip_address = socket.gethostbyname(hostname)
            module_name = func.__module__
            user_login = os.getlogin()  # Obtém o login do usuário do sistema operacional
            
            # Captura o arquivo onde a função está definida
            defined_in_file = getfile(func)
            # Captura o arquivo de onde a função foi chamada
            called_from_file = stack()[1].filename

            # Configurar um buffer temporário para capturar logs intermediários
            log_buffer = StringIO()
            handler = logging.StreamHandler(log_buffer)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logging.getLogger().addHandler(handler)

            status = "SUCCESS"
            error_message = None
            result = None

            try:
                # Executa a função decorada
                result = func(*args, **kwargs)
            except Exception as e:
                status = "FAILURE"
                error_message = str(e)
                logging.error(f"Erro na execução de '{function_name}': {e}", exc_info=True)
                raise
            finally:
                # Remover o handler temporário e obter mensagens capturadas
                logging.getLogger().removeHandler(handler)
                log_messages = log_buffer.getvalue()
                log_buffer.close()

                end_time = datetime.now()
                duration_seconds = (end_time - start_time).total_seconds()

                # Registrar no banco de dados se necessário
                if log_on_success or status == "FAILURE":
                    engine = get_engine('BI')
                    with engine.begin() as connection:
                        log_query = text("""
                        INSERT INTO tb_logs (
                            function_name, arguments, start_time, end_time, duration_seconds,
                            result, status, error_message, module_name, hostname, ip_address, custom_message,
                            log_messages, user_login, defined_in_file, called_from_file
                        ) VALUES (:function_name, :arguments, :start_time, :end_time, :duration_seconds,
                                  :result, :status, :error_message, :module_name, :hostname, :ip_address, :custom_message,
                                  :log_messages, :user_login, :defined_in_file, :called_from_file)
                        """)
                        connection.execute(log_query, {
                            'function_name': function_name,
                            'arguments': arguments,
                            'start_time': start_time,
                            'end_time': end_time,
                            'duration_seconds': duration_seconds,
                            'result': str(result) if result is not None else None,
                            'status': status,
                            'error_message': error_message,
                            'module_name': module_name,
                            'hostname': hostname,
                            'ip_address': ip_address,
                            'custom_message': custom_message,
                            'log_messages': log_messages,
                            'user_login': user_login,
                            'defined_in_file': defined_in_file,
                            'called_from_file': called_from_file,
                        })
                        logging.info(f"Log de execução de '{function_name}' registrado no banco de dados.")
            
            return result
        return wrapper
    return decorator
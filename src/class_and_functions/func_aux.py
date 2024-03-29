import re
from typing import Dict


def treat_sql_model(sql_path: str, var_globals: Dict[str, str]) -> str:
    """
    Treats SQL model by replacing variables in SQL text with corresponding values.
    Args:
        sql_path (str): The path to the SQL file.
        var_globals (Dict[str, str]): A dictionary containing variable-value mappings.
    Returns:
        str: The treated SQL text.
    """
    dict_globals = var_globals
    with open(sql_path, 'r') as sql_file:
        sql_text = sql_file.read()

    def find_sql_variables(text_sql: str) -> list:
        """
        Finds variables within SQL text.
        Args:
            text_sql (str): The SQL text.
        Returns:
            list: A list of found variables.
        """
        pattern = r'\{(.*?)\}'
        correspondences = re.findall(pattern, text_sql)
        correspondences = list(set(correspondences))
        correspondences = list(map(lambda element: element.replace('&', ''), correspondences))
        return correspondences 

    def find_variable_values(ls_variables: list) -> list:
        """
        Finds values for variables.
        Args:
            ls_variables (list): List of variables.
        Returns:
            list: List of corresponding values.
        """
        ls_values = []
        for element in ls_variables:
            value = dict_globals.get(element)
            ls_values.append(value)
        return ls_values

    ls_variables = find_sql_variables(sql_text)
    ls_values = find_variable_values(ls_variables)
    dict_variables = dict(zip(ls_variables, ls_values))

    treated_text = sql_text
    for key, value in dict_variables.items():
        treated_text = treated_text.replace(f"&{key}&", value)
        treated_text = treated_text.replace("{", "").replace("}", "")
    return treated_text


def treat_query_lake(minio_sql_minio, check_destination_folder, sql_query, destionation_bucket, minio_folder, partition_data_send):
    """
    Treats SQL query for Minio lake insertion.
    Args:
        minio_sql_minio: An instance of SQLMinio class.
        check_destination_folder: Flag to check if destination folder exists.
        sql_query: The SQL query to be treated.
        destionation_bucket: The destination bucket for Minio.
        minio_folder: The folder in Minio.
        partition_data_send: The data partition to be sent.
    Returns:
        str: The treated SQL query for Minio lake insertion.
    """
    if check_destination_folder:
        current_sql_lake = minio_sql_minio.generate_sql_union(destionation_bucket, minio_folder, partition_data_send)
        sel_union = minio_sql_minio.gerar_sql_uniao(sql_query, current_sql_lake)
        sql_insert_lake = minio_sql_minio.generate_sql_insert_minio_lake(sel_union, destionation_bucket, minio_folder)
        return sql_insert_lake
    else:
        sql_insert_lake = minio_sql_minio.generate_sql_insert_minio_lake(sql_query, destionation_bucket, minio_folder)
        return sql_insert_lake

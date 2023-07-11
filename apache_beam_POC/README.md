# APACHE BEAM POC


## Cloud Storage To BigQuery

![storage_to_bigquery](./images/storage_to_bigquery.png)

1. Pipeline_options define config's parameteres of apache_beam's pipeline:<br>
    1.1 runner<br>
    1.2 project<br>
    1.3 job_name<br>
    1.4 staging_location<br>
    1.5 region<br>
    1.6 template_location<br>
    1.7 save_main_session<br>

<br>

    External Objects:

        1° 'filtro' Class
        2° 'criar_dict_nivel' Function
        3° 'desanimar_dict' Function
        4° 'criar_dict_nivel10' Function

<br>

    Beam Objects:

        1° 'tempo_Atrasos'
        2° 'qtd_Atrasos'
        3° 'tabela_atrasos'


<br>

## Cloud Storage To Storage

![storage_to_storage](./images/storage_to_storage.png)


1. Pipeline_options define config's parameteres of apache_beam's pipeline:<br>
    1.1 runner<br>
    1.2 project<br>
    1.3 job_name<br>
    1.4 staging_location<br>
    1.5 temp_location<br>
    1.6 region<br>
    1.7 template_location<br>

<br>

    External Objects:

        1° 'filtro' Class

<br>

    Beam Objects:

        1° 'tempo_Atrasos'
        2° 'qtd_Atrasos'
        3° 'tabela_atrasos'


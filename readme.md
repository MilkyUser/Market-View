# Market-View

Este programa tem como função retornar um arquivo csv (impresso na saída padrão)
com as cotações diárias em um período específico de tempo de ativos listados na B3

## Instalação

Para instalar o programa é necessário ter instalado Python na versão 3.8 instalado
e executar os seguintes comandos: 

```
python -m venv venv

# Se no windows:
venv/Scripts/activate

# Se no linux:
source venv/bin/activate

pip install -r requirements.txt
```

## Input:

Para executar o programa é necessário utilizar os seguinte comando no terminal:

`python fetcher.py <data-de-inicio> <data-final> [lista-de-ações]`

## Output

O programa imprime na saída padrão o resultado em .csv seguindo o seguinte formato:

`DATA;[AÇÃO-A;AÇÃO-B; ... ;AÇÃO-N]`
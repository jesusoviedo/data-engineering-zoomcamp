# Clean Virtual Environments Script

Este script en Bash elimina los entornos virtuales creados con **pipenv** en directorios cuyo nombre comienza con `w` o `hw` (por ejemplo, directorios correspondientes a semanas de un curso). La idea es limpiar los entornos de semanas anteriores y dejar activo solo el que corresponde a la semana o tarea actual.

## Requisitos

- Tener instalado **pipenv**.
- El script asume que la estructura de directorios se organiza en el directorio padre del proyecto actual.
- Estar en la ubicación correcta antes de ejecutar el script (se utiliza `cd ..` al inicio).

## Uso

1. **Guardar el script**  
   Copia el contenido del script en un archivo llamado, por ejemplo, `clean_venv.sh`.

2. **Dar permisos de ejecución**  
   Ejecuta el siguiente comando para hacer el script ejecutable:
   ```bash
   chmod +x clean_venv.sh
   ```

3. **Ejecutar el script**  
    Para limpiar los entornos de los directorios que coincidan con el patrón `w*` o `hw*`, pero excluyendo aquellos que especifiques, ejecuta:
    ```bash
    ./clean_venv.sh directorio_excluido1 [directorio_excluido2 ...]
    ```
    
    Ejemplo:
    ```bash
    ./clean_venv.sh w1 hw1
    ```
    
    Esto procesará todos los directorios que comiencen con `w` o `hw` (ubicados en el directorio padre), eliminando el entorno virtual de pipenv en cada uno, excepto en los directorios `w1` y `hw1`.

## Cómo funciona

- **Verificación de argumentos:** si no se pasan argumentos, el script muestra un mensaje de uso y se detiene.

- **Listado de exclusiones:** los directorios a excluir se pasan como argumentos y se guardan en un array.

- **Función `is_excluded`:** comprueba si un directorio se encuentra en la lista de exclusión.

- **Recorrido de directorios:** el script se mueve al directorio padre (`cd ..`) y recorre cada directorio que empiece con `w` o `hw`.

    - Si el directorio está en la lista de exclusión, se salta.
    - Si no, entra en el directorio, verifica la existencia de un archivo `Pipfile` (indicativo de un entorno pipenv) y, de existir, ejecuta `pipenv --rm` para eliminar el entorno virtual.

- **Regreso al directorio raíz:** después de procesar cada directorio, vuelve al directorio raíz para continuar con el siguiente.

## Notas adicionales
- **Precaución:** asegúrate de que el script se ejecute desde la ubicación correcta, ya que utiliza `cd ..` para llegar a la carpeta que contiene los directorios `w*` y `hw*`.

- **Personalización:** puedes modificar el patrón de búsqueda o agregar funcionalidades adicionales según tus necesidades.
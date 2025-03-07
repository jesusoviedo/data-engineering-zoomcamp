#!/bin/bash

if [ "$#" -eq 0 ]; then
    echo "Uso: $0 directorio_excluido1 [directorio_excluido2 ...]"
    exit 1
fi

EXCLUDE_DIRS=("$@")

is_excluded() {
    local dir="$1"
    for exclude in "${EXCLUDE_DIRS[@]}"; do
        if [ "$dir" == "$exclude" ]; then
            return 0 
        fi
    done
    return 1
}

cd ..
for dir in w* hw*; do
    if [ -d "$dir" ]; then
        if is_excluded "$dir"; then
            echo "Saltando directorio excluido: $dir"
            continue
        fi

        echo "Procesando directorio: $dir"
        cd "$dir" || continue

        if [ -f "Pipfile" ]; then
            echo "Eliminando entorno virtual de pipenv en $dir"
            pipenv --rm
        else
            echo "No se encontró Pipfile en $dir, saltando..."
        fi

        cd ..
    fi
done
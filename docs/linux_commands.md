## 📂 Comandos básicos de Linux para trabajar con archivos, carpetas y permisos  


### ✅ 1. Descargar archivos  

```bash
wget <URL>
```

```bash
curl -o <archivo_destino> <URL>
```

---------------------------------


### ✅ 2. Descomprimir archivos 

```bash
gunzip <archivo>.gz        # Descomprimir archivos .gz
```

```bash
tar -xvf <archivo>.tar     # Extraer archivos .tar
```

```bash
tar -xzvf <archivo>.tar.gz # Extraer archivos .tar.gz
```

```bash
unzip <archivo>.zip        # Descomprimir archivos .zip
```

---------------------------------


### ✅ 3. Ver contenido de archivos

```bash
less <archivo>             # Ver contenido paginado
```

```bash
head -n 25 <archivo>       # Ver las primeras 25 líneas
```

```bash
tail -n 25 <archivo>       # Ver las últimas 25 líneas
```

```bash
cat <archivo>              # Mostrar todo el contenido (cuidado con archivos grandes)
```

---------------------------------


### ✅ 4. Mover archivos

```bash
mv <origen> <destino>
```

---------------------------------


### ✅ 5. Eliminar archivos y carpetas

```bash
rm <archivo>               # Eliminar archivo
```

```bash
rm -r <carpeta>            # Eliminar carpeta y su contenido
```

```bash
rm -rf <carpeta>           # Eliminar sin pedir confirmación (¡cuidado!)
```

---------------------------------


### ✅ 6. Renombrar archivos

```bash
mv <nombre_actual> <nombre_nuevo>
```

---------------------------------


### ✅ 7. Crear archivos y carpetas

```bash
touch <archivo>            # Crear un archivo vacío
```

```bash
mkdir <carpeta>            # Crear carpeta
```

```bash
mkdir -p ruta/otra/mas     # Crear estructura de carpetas (anidada)
```

---------------------------------


### ✅ 8. Copiar archivos y carpetas

```bash
cp <archivo_origen> <archivo_destino>       # Copiar archivo
```

```bash
cp -r <carpeta_origen> <carpeta_destino>    # Copiar carpeta y su contenido
```

---------------------------------


### ✅ 9. Permisos de archivos y carpetas

```bash
chmod 644 <archivo>        # Asignar permisos de lectura/escritura al propietario, lectura al resto
```

```bash
chmod +x <archivo>         # Dar permisos de ejecución
```

```bash
chown usuario:grupo <archivo>  # Cambiar propietario y grupo
```

```bash
ls -l                     # Ver permisos y propietarios de archivos y carpetas
```

---------------------------------


### ✅ 10. Listar archivos y carpetas

```bash
ls                        # Listar archivos en carpeta
```

```bash
ls -l                     # Listado detallado
```

```bash
ls -lh                    # Listado detallado con tamaños legibles
```

```bash
ls -a                     # Listar incluyendo archivos ocultos
```

---------------------------------


### ✅ 11. Buscar archivos y carpetas

```bash
find . -name "<nombre>"   # Buscar archivos o carpetas en el directorio actual y subdirectorios
```

```bash
grep "texto" <archivo>    # Buscar texto dentro de un archivo
```

```bash
grep -r "texto" <carpeta> # Buscar texto dentro de archivos en una carpeta
```

---------------------------------


### ✅ 12. Ver espacio en disco y uso

```bash
df -h                    # Ver espacio en disco
```

```bash
du -sh <carpeta>         # Ver tamaño de una carpeta específica
```

---------------------------------


## 🚀 Recomendación
Estos comandos cubren la mayoría de las tareas rutinarias cuando se trabaja con archivos, carpetas y permisos en Linux.

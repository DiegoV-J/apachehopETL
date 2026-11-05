@echo off
title Consultas MySQL - imdb_clean_final
color 0A

echo ============================================================
echo   Consola SQL sobre la tabla imdb_clean_final
echo ============================================================
echo.
echo   Escribe tus consultas y termina cada una con ;  (punto y coma)
echo   Para salir:  exit
echo.
echo   Ejemplo rapido:
echo     SELECT COUNT(*) FROM imdb_clean_final;
echo.

docker exec -i mysql-imdb mysql -u root -proot imdb

echo.
echo Sesion terminada.
pause

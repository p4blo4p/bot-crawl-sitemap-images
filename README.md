# bot-crawl-sitemap-images

# 🏹 ActionForge: Sitemap Hunter (mangas)

[![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-Automated-blue?logo=github-actions)](https://github.com/features/actions)
[![Python](https://img.shields.io/badge/Python-3.10+-yellow?logo=python)](https://www.python.org/)

**ActionForge: Sitemap Hunter** es una infraestructura de rastreo masivo optimizada para GitHub Actions.

## 📂 Arquitectura de Datos
Este proyecto utiliza un sistema de ramas dinámicas para mantener el repositorio limpio:
- **Ruta de Sitios**: `sites/mangas.txt`
- **Rama de Datos**: `mangas-sitemaps` (Contiene archivos .xml.gz y estadísticas)
- **Rama de Resultados**: `mangas-results` (Contiene los reportes de búsqueda)

## 🛠️ Workflows
1. **01 Download Sitemaps**: Descarga sitemaps usando `robots.txt`, clasifica el contenido y registra estadísticas de salud del dominio.
2. **02 Search Phrase**: Busca la frase **"Dragon Ball"** dentro de todos los sitemaps descargados de forma incremental.

## 🚀 Capacidades Pro
- **Limpieza de Disco Agresiva**: Libera espacio en el runner para soportar scans de gran volumen.
- **Git Resilience**: Configuración de red robusta para evitar timeouts en repositorios de datos grandes.
- **Circuit Breaker**: Detiene el rastreo de dominios con demasiados errores para ahorrar tiempo de ejecución.

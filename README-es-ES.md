# SPPD CLI

Herramienta de línea de comandos para descargar, extraer y convertir datos de contratación pública española al formato Parquet.

## Instalación

### Requisitos

- Rust 1.56 o superior

### Compilar desde el Código Fuente

```bash
git clone https://github.com/Alvaro2c/sppd-cli.git
cd sppd-cli
cargo build --release
```

El ejecutable estará disponible en `target/release/sppd-cli`.

## Uso

### Comando Básico

```bash
cargo run -- download [OPCIONES]
```

### Opciones

- `-t, --type <TIPO>`: Tipo de contratación (por defecto: `public-tenders`)
  - `public-tenders` (alias: `pt`, `pub`)
  - `minor-contracts` (alias: `mc`, `min`)
- `-s, --start <PERIODO>`: Período inicial (formato: `YYYY` o `YYYYMM`)
- `-e, --end <PERIODO>`: Período final (formato: `YYYY` o `YYYYMM`)
- `--cleanup <yes|no>`: Eliminar archivos intermedios (ZIP y XML/Atom) después del procesamiento, manteniendo solo los archivos Parquet (por defecto: `yes`)

**Períodos disponibles:**
- Años anteriores: solo años completos (`YYYY`)
- Año actual: todos los meses hasta la fecha de descarga (`YYYYMM`)

### Ejemplos

```bash
# Descargar todas las licitaciones públicas disponibles
cargo run -- download

# Descargar licitaciones públicas de 2023
cargo run -- download -t public-tenders -s 2023 -e 2023

# Descargar contratos menores de enero de 2025
cargo run -- download -t mc -s 202501 -e 202501

# Mantener archivos intermedios (sin limpiar)
cargo run -- download --cleanup no
```

### Salida

- Archivos ZIP: `data/tmp/{mc,pt}/`
- Archivos Parquet: `data/parquet/{mc,pt}/`

### Registro

Controla los niveles de registro con `RUST_LOG`:

```bash
RUST_LOG=debug cargo run -- download  # Salida detallada
RUST_LOG=warn cargo run -- download   # Solo advertencias y errores
```

## Contribuciones

¡Las contribuciones son bienvenidas! Por favor, siéntete libre de enviar una Pull Request.

## Licencia

Este proyecto está bajo doble licencia, puedes elegir entre:

- Licencia Apache, Versión 2.0 ([LICENSE](LICENSE) o http://www.apache.org/licenses/LICENSE-2.0)
- Licencia MIT (http://opensource.org/licenses/MIT)


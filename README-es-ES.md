# SPPD CLI

Una herramienta de línea de comandos para descargar, extraer y convertir datos de contratación pública española a formato Parquet.

## Instalación

### Requisitos previos

- Rust 1.56 o superior

### Compilar desde el código fuente

```bash
git clone https://github.com/Alvaro2c/sppd-cli.git
cd sppd-cli
cargo build --release
```

El binario estará disponible en `target/release/sppd-cli`.

## Documentación

Documentación : https://alvaro2c.github.io/sppd-cli/sppd_cli/

## Uso como Librería

Añade la dependencia desde GitHub:

```toml
sppd-cli = { git = "https://github.com/Alvaro2c/sppd-cli" }
```

```rust
use sppd_cli::{downloader, extractor, parser};
```

## Arquitectura

```
Downloader -> Extractor -> Parser -> Parquet
    |             |           |
  obtiene     descomprime   parsea XML
  enlaces ZIP archivos ZIP  a DataFrame
```

## Uso

### CLI Manual

```bash
cargo run -- cli [OPCIONES]
```

### Opciones

- `-t, --type <TIPO>`: Tipo de contratación (por defecto: `public-tenders`)
  - `public-tenders` (alias: `pt`, `pub`)
  - `minor-contracts` (alias: `mc`, `min`)
- `-s, --start <PERIODO>`: Período inicial (formato: `YYYY` o `YYYYMM`)
- `-e, --end <PERIODO>`: Período final (formato: `YYYY` o `YYYYMM`)

La limpieza (`cleanup`) está activada siempre en la CLI manual. Usa un archivo TOML si necesitas cambiar ese comportamiento.

**Períodos disponibles:**
- Años anteriores: solo años completos (`YYYY`)
- Año actual: todos los meses hasta la fecha de descarga (`YYYYMM`)

### Configuración TOML

```bash
cargo run -- toml config/prod.toml
```

El archivo TOML te permite definir tanto los parámetros de ejecución (`type`, `start`, `end`, `cleanup`) como los valores por defecto de la canalización (tamaño de lote, reintentos, rutas, etc.). Por ejemplo:

```toml
type = "public-tenders"
start = "202301"
end = "202312"
cleanup = false

batch_size = 100
concurrent_downloads = 4
retry_initial_delay_ms = 1000
retry_max_delay_ms = 10000

download_dir_mc = "data/tmp/mc"
download_dir_pt = "data/tmp/pt"
parquet_dir_mc = "data/parquet/mc"
parquet_dir_pt = "data/parquet/pt"
```

Solo `type`, `start` y `end` son obligatorios; el resto hereda los valores integrados.

### Variables de Entorno

- `RUST_LOG`: Nivel de registro (`debug`, `info`, `warn`)

### Ejemplos

```bash
# Descarga manual con limpieza activada y valores por defecto
cargo run -- cli -t public-tenders -s 2023 -e 2023

# Ejecuta con un archivo TOML (para orquestación)
cargo run -- toml config/prod.toml
```

### Salida

- Archivos ZIP: `data/tmp/{mc,pt}/`
- Archivos Parquet: `data/parquet/{mc,pt}/`

### Registro

Controla los niveles de registro con `RUST_LOG`:

```bash
RUST_LOG=debug cargo run -- cli  # Salida detallada
RUST_LOG=warn cargo run -- cli   # Solo advertencias y errores
```

## Contribuciones

¡Las contribuciones son bienvenidas! Por favor, siéntete libre de enviar una Pull Request.

## Licencia

Este proyecto está bajo doble licencia, puedes elegir entre:

- Licencia Apache, Versión 2.0 ([LICENSE](LICENSE) o http://www.apache.org/licenses/LICENSE-2.0)
- Licencia MIT (http://opensource.org/licenses/MIT)

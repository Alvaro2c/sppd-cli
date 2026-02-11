# SPPD CLI

Una herramienta de línea de comandos para descargar, extraer y convertir datos de contratación pública española a formato Parquet.

## Instalación

### Descargar desde Releases

Hay binarios precompilados en la página de [Releases](https://github.com/Alvaro2c/sppd-cli/releases).

1. Descarga el archivo para tu plataforma:
   - **Linux** (x86_64): `sppd-cli-linux-x86_64-v*.tar.gz`
   - **macOS** (Apple Silicon): `sppd-cli-macos-aarch64-v*.tar.gz`
   - **Windows** (x86_64): `sppd-cli-windows-x86_64-v*.zip`
2. Extrae y ejecuta:
   - Linux/macOS: `tar xzf sppd-cli-*.tar.gz` y luego `./sppd-cli --help`
   - Windows: descomprime el archivo y ejecuta `.\sppd-cli.exe --help`
3. (Opcional) Añade el binario a tu PATH para usarlo globalmente.

### Compilar desde el código fuente

**Requisitos previos:** Rust 1.56 o superior

```bash
git clone https://github.com/Alvaro2c/sppd-cli.git
cd sppd-cli
cargo build --release
```

El binario estará disponible en `target/release/sppd-cli`.

## Documentación

La documentación se publica en GitHub Pages: https://alvaro2c.github.io/sppd-cli/sppd_cli/

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
sppd-cli cli [OPCIONES]
```

*(Al compilar desde el código fuente, usa `cargo run -- cli [OPCIONES]` en su lugar.)*

### Opciones

- `-t, --type <TIPO>`: Tipo de contratación (por defecto `public-tenders` junto con una advertencia)
  - `public-tenders` (alias: `pt`, `pub`)
  - `minor-contracts` (alias: `mc`, `min`)
- `-s, --start <PERIODO>`: Período inicial (formato: `YYYY` o `YYYYMM`)
- `-e, --end <PERIODO>`: Período final (formato: `YYYY` o `YYYYMM`)
- `-b, --batch-size <N>` (alias `--bs`): Número de archivos XML a procesar por lote (por defecto: `150`; afecta a la memoria máxima)
- `-r, --read-concurrency <N>` (alias `--rc`): Número de archivos XML leídos en paralelo durante el parsing (por defecto: `16`)
- `--parser-threads <N>` (alias `--pt`): Número de hilos del pool rayon para el parsing XML (por defecto: 0 = auto; útil en Docker para igualar el límite de CPU del contenedor)
- `-c, --concat-batches` (alias `--cb`): Fusiona los archivos Parquet por lotes en un único archivo por período (precaución: alto uso de memoria en períodos grandes)
- `--no-cleanup`: Salta la limpieza de archivos ZIP descargados y directorios extraídos (limpieza habilitada por defecto)
- `--keep-cfs-raw-xml`: Incluye el XML bruto de ContractFolderStatus en la salida de Parquet (deshabilitado por defecto para eficiencia de memoria)

**Períodos disponibles:**
- Años anteriores: solo años completos (`YYYY`)
- Año actual: todos los meses hasta la fecha de descarga (`YYYYMM`)

### Configuración TOML

```bash
sppd-cli toml config/prod.toml
```

*(Al compilar desde el código fuente, usa `cargo run -- toml config/prod.toml` en su lugar.)*

El archivo TOML te permite declarar los parámetros de ejecución de la CLI y, opcionalmente, anular cualquiera de los valores predeterminados de la canalización. El parser falla si omites una clave **obligatoria** (`type`, `start` o `end`) o incluyes una clave desconocida (las erratas se rechazan). Todo lo demás usa los valores incorporados a menos que lo cambies.

Campos obligatorios:

- `type`: `public-tenders` (`pt`, `pub`) o `minor-contracts` (`mc`, `min`)
- `start`: periodo en formato `YYYY` o `YYYYMM`
- `end`: periodo en formato `YYYY` o `YYYYMM`

Overrides opcionales:

- `cleanup` (bool, por defecto `true`)
- `keep_cfs_raw_xml` (bool, por defecto `false`)
- Valores por defecto de la canalización:
  - `batch_size` (archivos XML por lote al parsear; por defecto `150`; limita la memoria máxima del DataFrame)
  - `read_concurrency` (archivos XML leídos en paralelo; por defecto `16`)
  - `parser_threads` (tamaño del pool rayon para parsing XML; por defecto `0` = auto vía available_parallelism(); en Docker, igualar al límite de CPU del contenedor)
  - `concat_batches` (bool, por defecto `false`; fusiona los Parquet por lotes en un único archivo por período; precaución: alto uso de memoria en períodos grandes)
  - `max_retries` (por defecto `3`)
  - `retry_initial_delay_ms` (por defecto `1000`)
  - `retry_max_delay_ms` (por defecto `10000`)
  - `concurrent_downloads` (por defecto `4`)
  - `download_dir_mc`, `download_dir_pt`
  - `parquet_dir_mc`, `parquet_dir_pt`

Ejemplo:

```toml
type = "public-tenders"
start = "202501"
end = "202502"
cleanup = false
keep_cfs_raw_xml = false

batch_size = 150
read_concurrency = 16
parser_threads = 0
concat_batches = false
max_retries = 5
retry_initial_delay_ms = 1000
retry_max_delay_ms = 10000
concurrent_downloads = 4

download_dir_mc = "data/tmp/mc"
download_dir_pt = "data/tmp/pt"
parquet_dir_mc = "data/parquet/mc"
parquet_dir_pt = "data/parquet/pt"
```

### Variables de Entorno

- `RUST_LOG`: Nivel de registro (`debug`, `info`, `warn`)

### Ejemplos

```bash
# Descarga manual con limpieza activada y valores por defecto
sppd-cli cli -t public-tenders -s 2023 -e 2023

# Ejecuta con un archivo TOML (para orquestación)
sppd-cli toml config/prod.toml
```

*(Al compilar desde el código fuente, usa `cargo run -- cli` o `cargo run -- toml` en lugar de `sppd-cli cli` o `sppd-cli toml`.)*

### Salida

- Archivos ZIP: `data/tmp/{mc,pt}/`
- Archivos Parquet: `data/parquet/{mc,pt}/`

### Esquema de salida

Cada registro Parquet refleja un `<entry>` de Atom más los datos extraídos de `ContractFolderStatus`.

| Columna | Descripción |
|---------|-------------|
| `id` | ID del `<entry>` |
| `title` | Título del entry |
| `link` | URL del enlace |
| `summary` | Resumen |
| `updated` | Fecha de última actualización |
| `status` | Struct que agrupa `<cbc-place-ext:ContractFolderStatusCode>` con los campos `code` y `list_uri`. |
| `contract_id` | `<cbc:ContractFolderID>` |
| `contracting_party` | Struct que agrupa la metadata de la entidad adjudicadora. Contiene `name`, `website`, `type_code`, `type_code_list_uri`, `activity_code`, `activity_code_list_uri`, `city`, `zip`, `country_code` y `country_code_list_uri`. |
| `project` | Struct que reúne los campos del proyecto sin lotes (`name`, `type_code`, `type_code_list_uri`, `sub_type_code`, `sub_type_code_list_uri`, `total_amount`, `total_currency`, `tax_exclusive_amount`, `tax_exclusive_currency`, `cpv_code`, `cpv_code_list_uri`, `country_code`, `country_code_list_uri`). `project.cpv_code` sigue concatenando varios `<cbc:ItemClassificationCode>` con `_`. |
| `project_lots` | Lista de structs `<cac:ProcurementProjectLot>`, cada una con `id`, `name`, importes presupuestarios con sus monedas, `cpv_code`/`cpv_code_list_uri` concatenados, y código de país con su `country_code_list_uri`. |
| `tender_results` | Lista de structs generadas a partir de `<cac:TenderResult>`. Cada entrada tiene `result_id` (contador artificial por TenderResult en orden de documento), `result_lot_id` (identificador del lote o `0` si no hay lotes) y los campos: `result_code`, `result_code_list_uri`, `result_description`, `result_winning_party`, `result_sme_awarded_indicator`, `result_award_date`, `result_tax_exclusive_amount`, `result_tax_exclusive_currency`, `result_payable_amount` y `result_payable_currency`. |
| `terms_funding_program` | Struct que agrupa `<cac:TenderingTerms>/<cbc:FundingProgramCode>` con los campos `code` y `list_uri`. |
| `process` | Struct con los valores de `<cac:TenderingProcess>` (`end_date`, `procedure_code`, `procedure_code_list_uri`, `urgency_code`, `urgency_code_list_uri`). |
| `cfs_raw_xml` | XML completo de `<cac-place-ext:ContractFolderStatus>`. Solo se rellena cuando se establece `--keep-cfs-raw-xml` (deshabilitado por defecto para eficiencia de memoria). |

Los valores múltiples para el mismo campo se concatenan con `_` (p. ej., `project.cpv_code` y cada `cpv_code` dentro de los lotes).

> **Especificación de Formato XML**: Para información detallada sobre la estructura XML, definiciones de campos, y para solicitar o proponer nuevos campos en el parser, consulta la especificación oficial [Formato de sindicación y reutilización de datos](https://contrataciondelsectorpublico.gob.es/datosabiertos/especificacion-sindicacion.pdf) de la Plataforma de Contratación del Sector Público.

### Ajuste de Memoria

El parser escribe archivos Parquet por lotes para que cada período solo tenga en memoria `batch_size` entradas a la vez. Configura los siguientes parámetros según los recursos disponibles:

| Parámetro | Por defecto | Recomendado Docker/Airflow | Efecto |
|-----------|-------------|----------------------------|--------|
| `batch_size` | 150 | 50-100 | Control principal de memoria. Valores más bajos reducen la memoria a costa de más archivos Parquet. Cada lote = O(batch_size × tamaño_medio_entrada) en memoria. |
| `read_concurrency` | 16 | 4-8 | Controla la I/O simultánea de archivos XML. Valores más bajos reducen la presión sobre el almacenamiento. |
| `parser_threads` | 0 (auto) | 2-4 | Tamaño del pool de hilos rayon para el parsing XML en paralelo. En Docker, iguala este valor al límite de CPU del contenedor (p. ej. 2 para 2 núcleos). El valor 0 auto-detecta con available_parallelism(), que puede devolver los núcleos del host en lugar del contenedor y provocar sobresuscripción. |
| `concat_batches` | false | false | Si está activo, los archivos por lotes se fusionan en uno por período en memoria. Úsalo solo si el período entero cabe en RAM. Desactívalo en Docker con límites de memoria ajustados. |

#### Ejemplo: contenedor Docker con 2 GB RAM y 2 núcleos CPU

```bash
sppd-cli cli -t pt -s 2024 -e 2024 -b 50 -r 4 --parser-threads 2
```

*(Al compilar desde el código fuente: `cargo run -- cli ...`.)*

O vía TOML:

```toml
type = "pt"
start = "2024"
end = "2024"
batch_size = 50
read_concurrency = 4
parser_threads = 2
concat_batches = false
```

Esta configuración:
- Procesa 50 archivos XML por lote, limitando el DataFrame en memoria a ~500-1000 MB
- Lee 4 archivos en paralelo, reduciendo la contención de I/O
- Usa exactamente 2 hilos de parser (igual que el límite de CPU del contenedor)
- Genera varios archivos por lote por período en lugar de concatenar (ahorra memoria)

Estructura de salida:
- Por defecto: `data/parquet/{mc,pt}/{period}/batch_*.parquet`
- Con `concat_batches`: `data/parquet/{mc,pt}/{period}.parquet`

#### Notas de rendimiento

- **Pool Rayon acotado**: El parser usa un pool de hilos que respeta `parser_threads`, evitando la sobresuscripción del pool global en contenedores.
- **Streaming eficiente en memoria**: El parsing XML es por streaming (estilo SAX), no basado en DOM, minimizando el uso de memoria por archivo.
- **Liberación temprana de memoria**: Los bytes XML en bruto se descartan tras el parsing, antes de construir el DataFrame, minimizando las asignaciones simultáneas.

### Registro

Controla los niveles de registro con `RUST_LOG`:

```bash
RUST_LOG=debug sppd-cli cli  # Salida detallada
RUST_LOG=warn sppd-cli cli   # Solo advertencias y errores
```

*(Al compilar desde el código fuente: `RUST_LOG=debug cargo run -- cli`.)*

## Contribuciones

¡Las contribuciones son bienvenidas! Por favor, siéntete libre de enviar una Pull Request.

## Licencia

Este proyecto está bajo doble licencia, puedes elegir entre:

- Licencia Apache, Versión 2.0 ([LICENSE](LICENSE) o http://www.apache.org/licenses/LICENSE-2.0)
- Licencia MIT (http://opensource.org/licenses/MIT)

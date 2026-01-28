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
- `-r, --read-concurrency <N>` (alias `--rc`): Número de archivos XML leídos en paralelo durante el parsing (por defecto: `16`)
- `-c, --concat-batches` (alias `--cb`): Fusiona los archivos Parquet por lotes en un único archivo por período

La limpieza (`cleanup`) está activada siempre en la CLI manual. Usa un archivo TOML si necesitas cambiar ese comportamiento.

**Períodos disponibles:**
- Años anteriores: solo años completos (`YYYY`)
- Año actual: todos los meses hasta la fecha de descarga (`YYYYMM`)

### Configuración TOML

```bash
cargo run -- toml config/prod.toml
```

El archivo TOML te permite declarar los parámetros de ejecución de la CLI y, opcionalmente, anular cualquiera de los valores predeterminados de la canalización. El parser falla si omites una clave **obligatoria** (`type`, `start` o `end`) o incluyes una clave desconocida (las erratas se rechazan). Todo lo demás usa los valores incorporados a menos que lo cambies.

Campos obligatorios:

- `type`: `public-tenders` (`pt`, `pub`) o `minor-contracts` (`mc`, `min`)
- `start`: periodo en formato `YYYY` o `YYYYMM`
- `end`: periodo en formato `YYYY` o `YYYYMM`

Overrides opcionales:

- `cleanup` (bool, por defecto `true`)
- Valores por defecto de la canalización:
-  - `batch_size` (número de archivos por lote al parsear; por defecto `150`; controla la memoria máxima)
-  - `read_concurrency` (número de archivos XML leídos en paralelo; por defecto `16`)
-  - `concat_batches` (bool, por defecto `false`; fusiona los archivos Parquet por lotes en uno solo)
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

batch_size = 150
read_concurrency = 16
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
cargo run -- cli -t public-tenders -s 2023 -e 2023

# Ejecuta con un archivo TOML (para orquestación)
cargo run -- toml config/prod.toml
```

### Salida

- Archivos ZIP: `data/tmp/{mc,pt}/`
- Archivos Parquet: `data/parquet/{mc,pt}/`

### Esquema de salida

Cada registro Parquet refleja un `<entry>` de Atom más los datos extraídos de `ContractFolderStatus`, así que contiene 27 columnas:

| Columna | Descripción |
|---------|-------------|
| `id` | ID del `<entry>` |
| `title` | Título del entry |
| `link` | URL del enlace |
| `summary` | Resumen |
| `updated` | Fecha de última actualización |
| `status_code` | `<cbc-place-ext:ContractFolderStatusCode>` |
| `status_code_list_uri` | Atributo `listURI` de `status_code` |
| `contract_id` | `<cbc:ContractFolderID>` |
| `contracting_party` | Estructura que agrupa la metadata de la entidad adjudicadora. Contiene `name`, `website`, `type_code`, `type_code_list_uri`, `activity_code`, `activity_code_list_uri`, `city`, `zip`, `country_code` y `country_code_list_uri`. |
| `project` | Estructura que reúne los campos del proyecto sin lotes (`name`, `type_code`, `type_code_list_uri`, `sub_type_code`, `sub_type_code_list_uri`, `total_amount`, `total_currency`, `tax_exclusive_amount`, `tax_exclusive_currency`, `cpv_code`, `cpv_code_list_uri`, `country_code`, `country_code_list_uri`). `project.cpv_code` sigue concatenando varios `<cbc:ItemClassificationCode>` con `_`. |
| `project_lots` | Lista de estructuras `<cac:ProcurementProjectLot>`, cada una con `id`, `name`, importes presupuestarios con sus monedas, `cpv_code`/`cpv_code_list_uri` concatenados, y código de país con su `country_code_list_uri`. |
| `result_code` | `<cac:TenderResult>/<cbc:ResultCode>` |
| `result_code_list_uri` | Atributo `listURI` del código de resultado |
| `result_description` | `<cac:TenderResult>/<cbc:Description>` |
| `result_winning_party` | `<cac:TenderResult>/<cac:WinningParty>/<cac:PartyName>/<cbc:Name>` |
| `result_sme_awarded_indicator` | `<cac:TenderResult>/<cbc:SMEAwardedIndicator>` |
| `result_award_date` | `<cac:TenderResult>/<cbc:AwardDate>` |
| `result_tax_exclusive_amount` | Valor de `<cac:AwardedTenderedProject>/<cac:LegalMonetaryTotal>/<cbc:TaxExclusiveAmount>` |
| `result_tax_exclusive_currency` | Atributo `currencyID` del monto sin IVA |
| `result_payable_amount` | Valor de `<cac:AwardedTenderedProject>/<cac:LegalMonetaryTotal>/<cbc:PayableAmount>` |
| `result_payable_currency` | Atributo `currencyID` del importe pagadero |
| `terms_funding_program_code` | `<cac:TenderingTerms>/<cbc:FundingProgramCode>` |
| `terms_funding_program_code_list_uri` | Atributo `listURI` del código del programa de financiación |
| `terms_award_criteria_type_code` | `<cac:TenderingTerms>/<cac:AwardingTerms>/<cac:AwardingCriteria>/<cbc:AwardingCriteriaTypeCode>` |
| `terms_award_criteria_type_code_list_uri` | Atributo `listURI` del código del criterio de adjudicación |
| `process` | Estructura con los valores de `<cac:TenderingProcess>` (`end_date`, `procedure_code`, `procedure_code_list_uri`, `urgency_code`, `urgency_code_list_uri`). |
| `cfs_raw_xml` | XML completo de `<cac-place-ext:ContractFolderStatus>` |

Los valores múltiples para el mismo campo se concatenan con `_` (p. ej., `project.cpv_code` y cada `cpv_code` dentro de los lotes).

### Ajuste de Memoria

El parser escribe archivos Parquet por lotes para que cada período solo tenga en memoria `batch_size` entradas a la vez. Configura los siguientes parámetros según los recursos disponibles:

| Parámetro | Por defecto | Efecto |
|-----------|-------------|--------|
| `batch_size` | 150 | Control principal. Valores más bajos reducen la memoria a costa de más archivos Parquet. |
| `read_concurrency` | 16 | Cuántos archivos XML se leen simultáneamente. Reduce este valor si el almacenamiento es lento o la RAM escasa. |
| `concat_batches` | false | Fusiona los archivos por lotes y genera `data/parquet/{mc,pt}/{period}.parquet`. Úsalo solo si el período entero cabe en RAM. |

Estructura de salida:
- Por defecto: `data/parquet/{mc,pt}/{period}/batch_*.parquet`
- Con `concat_batches`: `data/parquet/{mc,pt}/{period}.parquet`

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

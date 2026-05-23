# PDF Processing Pipeline — CY (Coyote Logistics)

Taller Databricks — Parte 1 | Maestria en Ciencia de Datos  
**Autor:** Yan Caicedo  
**Rama principal:** `dev`
**Materia: Procesamiento en Nube/DataBricks**
**MCD ICESI**

---

## Descripcion

Pipeline de procesamiento de PDFs para la fuente **CY (Coyote Logistics LLC)**.  
Convierte load confirmations en formato PDF a registros estructurados almacenados en una tabla Delta en Databricks.

---

## Estructura del repositorio

```
pdf_processing_CY_Yan_Caicedo/
├── pdf_processing_cy.yml                          # Workflow de Databricks
└── src/
    └── pipelines/
        └── pdf_processing/
            ├── pdf_to_text_with_pymupdf4llm.py    # Tarea 1: PDF -> TXT (Markdown)
            ├── text_to_columns_cy.py              # Tarea 2: TXT -> Delta Table
            └── text_to_columns_validator_cy.py    # Tarea 3: Validacion de campos
```

---

## Pipeline

El workflow tiene 3 tareas que se ejecutan en secuencia:

| Tarea | Script | Descripcion |
|---|---|---|
| `pdf_to_txt` | `pdf_to_text_with_pymupdf4llm.py` | Convierte cada PDF a TXT en formato Markdown usando pymupdf4llm |
| `txt_to_columns` | `text_to_columns_cy.py` | Extrae campos estructurados y escribe en la tabla Delta |
| `txt_to_columns_validator` | `text_to_columns_validator_cy.py` | Valida los campos extraidos contra un ground truth |

### Parametros del workflow

| Parametro | Valor por defecto |
|---|---|
| `base_path` | `/Volumes/logistics/default/raw` |
| `source_name` | `CY` |
| `loads_table` | `logistics.default.truckr_loads_cy` |

---

## Dependencias

```
PyMuPDF==1.26.4
pymupdf4llm==0.0.27
```

Declaradas en el entorno `dev` del workflow YAML.

---

## Metodo de conversion

Se utiliza **pymupdf4llm** para convertir los PDFs a texto en formato Markdown.  
Esta libreria fue seleccionada sobre pdfplumber porque preserva la estructura de secciones del documento (Stop 1: Pick Up, Stop 2: Delivery, Agreement), lo que facilita la extraccion de campos con expresiones regulares orientadas a ese formato.

---

## Tabla destino

`logistics.default.truckr_loads_cy` — tabla Delta en Databricks Unity Catalog.

---

## Politica de privacidad

- Los PDFs originales **no se incluyen** en este repositorio.
- El validador **no incluye** en su ground truth datos de personas naturales (`broker_rep`, `broker_rep_phone`). Estos campos son extraidos y almacenados por el pipeline pero omitidos del repositorio publico conforme a las indicaciones del taller.
- Solo se suben scripts que no contienen datos sensibles de personas naturales.

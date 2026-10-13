# Changelog

## 2026-10-13

### Fix: un pendiente cuyo modal no cargaba perdía su auth code y su clasificación

**Síntoma:** un pendiente que ya clasificaste aparece de nuevo sin rubro, sin comercio y sin clasificar, como si fuera otro movimiento. En la DB hay una sola fila.

**Causa raíz:** `_reset_pending()` borra todos los pendientes al inicio de cada corrida y los vuelve a insertar con lo que traiga el modal. Si en una corrida el modal de ese pendiente sale vacío (`detalle vacío` en el log), se re-inserta sin `codigo_autorizacion` y con `tx_hash`. La clasificación, que estaba guardada por auth code, queda apuntando a un auth code que ya no existe en `movimientos`. El modal vacío ocurre en una fila al azar casi todas las corridas.

La entrada del 12/10 decía que los pendientes con el modal vacío no eran ese bug. Este es el bug que sí les afecta.

**Fix (`scraper/bank_scraper.py`):**
- `_open_and_read_detail` reintenta una vez cuando el modal sale vacío.
- Además, antes de extraer, `_open_and_read_detail` espera a que `JS_EXTRACT_FIELDS` ubique el modal. La espera previa terminaba apenas aparecía la etiqueta `Comercio`, que se renderiza antes que los pares de etiquetas que la extracción usa como ancla, así que en CI se extraía a medio renderizar y el modal salía vacío una y otra vez.
- `_save_pending_details()` guarda antes del reset los campos del modal de los pendientes con auth code: auth, rubro, comercio, fecha_compra, hora, país y origen. Si el modal falla también en el reintento, `_restore_pending_detail()` los recupera. Solo lo hace si hay exactamente un pendiente guardado con esa descripción y monto: con dos no se sabe cuál es cuál, y asignar mal el auth code haría que uno pise al otro.

**Limpieza de DB:** no hace falta. La clasificación huérfana se vuelve a enganchar sola cuando el modal entregue de nuevo el auth code. Para encontrar clasificaciones huérfanas:

```sql
SELECT c.* FROM clasificaciones c
 WHERE c.codigo_autorizacion IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM movimientos m WHERE m.codigo_autorizacion = c.codigo_autorizacion);
```

Ojo: un pendiente que hoy está guardado sin auth code por este bug aparece aquí hasta que se recupere. No borres esas filas sin revisar.

## 2026-10-12

### Fix: un confirmado cuyo modal no cargó quedaba duplicado al completarse

**Síntoma:** en Análisis y Clasificación aparece un movimiento confirmado sin fecha y sin comercio, y el mismo cargo aparece de nuevo, completo y con `codigo_autorizacion`. La fila vacía no tiene auth code, `fecha_compra`, rubro ni comercio, y se identifica por `tx_hash`.

**Causa raíz:** cuando el modal de un confirmado no carga (`detalle vacío` en el log), la fila se guarda con `tx_hash` calculado sobre `fecha|periodo_facturacion`, porque no hay `fecha_compra`. En la siguiente corrida se re-procesa por estar en `incomplete_keys`. Si esta vez el modal trae auth code y `fecha_compra`, `_upsert_to_db` intenta borrar la fila previa buscando por `potential_hash`, que ahora se calcula con `fecha_compra`. Los hashes no coinciden, el DELETE no borra nada y el INSERT por `(codigo_autorizacion, num_cuotas)` crea una segunda fila. La duplicada sigue en `incomplete_keys`, así que se re-procesa en cada corrida.

**Fix (`scraper/bank_scraper.py`):** el DELETE previo al upsert con auth code busca los dos hashes posibles: el de `fecha_compra` y el de respaldo `fecha|periodo_facturacion`. El hash que borra se pasa al bloque de migración, para que una clasificación hecha sobre la fila vacía siga al auth code.

Los **pendientes** sin fecha y con el modal vacío no son este bug: `_reset_pending()` los borra en la próxima corrida y se vuelven a cargar.

**Limpieza de DB:** busca filas sin auth code que tengan una gemela con auth code:

```sql
SELECT a.id AS id_hash, b.id AS id_auth, a.fecha, a.descripcion, a.monto, a.num_cuotas,
       a.periodo, a.tx_hash, b.codigo_autorizacion
  FROM movimientos a
  JOIN movimientos b
    ON a.codigo_autorizacion IS NULL AND a.tx_hash IS NOT NULL
   AND b.codigo_autorizacion IS NOT NULL
   AND a.fecha = b.fecha AND upper(a.descripcion) = upper(b.descripcion)
   AND a.monto = b.monto
   AND a.num_cuotas IS NOT DISTINCT FROM b.num_cuotas
   AND a.periodo    IS NOT DISTINCT FROM b.periodo;
```

Para cada par, revisa que la fila `id_auth` tenga `fecha_compra`, rubro y comercio. Si la clasificación del `tx_hash` no existe para el auth code, mígrala. Si ya existe, bórrala. Después borra la fila vacía:

```sql
-- solo si no hay clasificación para el auth code:
UPDATE clasificaciones SET codigo_autorizacion = '<auth>', tx_hash = NULL WHERE tx_hash = '<hash>';
UPDATE splits          SET codigo_autorizacion = '<auth>', tx_hash = NULL WHERE tx_hash = '<hash>';
-- en cualquier caso:
DELETE FROM clasificaciones WHERE tx_hash = '<hash>';
DELETE FROM splits          WHERE tx_hash = '<hash>';
DELETE FROM movimientos     WHERE id = <id_hash>;
```

## 2026-09-26

### Fix: un pendiente sin código de autorización se guardaba como confirmado (duplicado)

**Síntoma:** un mismo cargo aparece dos veces en el período abierto: una fila confirmada con fecha, y otra **sin fecha**, sin rubro, hora `00:00` y `pendiente = FALSE`, a veces con una descripción más corta (ej. `COMPRA APPLE` vs `COMPRA APPLE.COM/BILL`). Ninguna de las dos tiene `codigo_autorizacion`.

**Causa raíz:** `extract_all_movements` decidía `pendiente` según la estructura que sirviera el banco, y la rama *"sin auth code pero el modal cargó"* (la de abril–mayo 2026) usa `Cuotas` presente = confirmado. En la estructura actual los pendientes **también** traen `Cuotas` (`01/01`), así que un pendiente cuyo modal llegó sin auth code se guardaba como confirmado aunque la tabla no le mostrara fecha. Una vez así queda fuera del alcance de `_reset_pending()` para siempre, y cuando el banco lo confirma con otra descripción el hash cambia y entra como fila nueva.

**Fix (`scraper/bank_scraper.py`):** *sin fecha en la tabla = pendiente* se evalúa primero, antes de mirar el modal. En la estructura de abril–mayo los pendientes sí mostraban fecha, así que ahí no cambia nada; las filas con fecha siguen la misma lógica de antes.

**Limpieza de DB:** revisar primero —debería devolver cero filas o solo pendientes mal marcados:

```sql
SELECT id, descripcion, monto, fecha_compra, codigo_autorizacion, tx_hash, periodo
  FROM movimientos WHERE fecha IS NULL AND pendiente = FALSE;
```

Para cada fila, confirmar que exista su versión confirmada (misma compra, con fecha) antes de borrar. Si la clasificaste, clasifica también la confirmada. Después borra la fila y lo que dependa de su `tx_hash`:

```sql
DELETE FROM splits          WHERE tx_hash = '<hash>';
DELETE FROM clasificaciones WHERE tx_hash = '<hash>';
DELETE FROM movimientos     WHERE id = <id>;
```

**Hueco conocido, sin fix:** si un pendiente sin auth code cambia de **descripción** al confirmarse, su clasificación no se migra: el bloque de migración de `_save_movement` cubre cambios de `fecha_compra`, no de descripción. Hay que volver a clasificarlo.

### Fix: "No pudimos cargar tus últimos movimientos" hacía fallar la corrida

**Síntoma:** la corrida termina en `Navegación a movimientos fallida` / `No apareció la tabla`, y el screenshot `movements_table_not_found.png` muestra el error transitorio del banco en *Últimos movimientos* con su botón **Reintentar**.

**Fix:** `navigate_to_movements` ahora espera las filas con `_wait_last_movements()`, que detecta ese estado en `app-last-movements` y hace clic en Reintentar hasta 3 veces, igual que ya se hacía en la pestaña de facturados.

### Diagnóstico: el screenshot de un login fallido no mostraba el mensaje del banco

`login_failed.png` se toma ~30 s después del submit, con el drawer ya cerrado. Ahora, si el login falla, también se guardan `login_after_submit.png` y el texto del drawer (en el log) 2,5 s después del submit.

## 2026-09-21 (2)

### Fix: dos cobros idénticos el mismo día se guardaban como uno solo

**Síntoma:** la reconciliación de `2026-09` daba $4.505.274 contra los $4.507.930 que informa el banco. El estado de cuenta trae **108 filas** y la DB tenía **106**; sumando las filas del propio estado de cuenta da $4.510.274, que menos la `DEVOLUCION COMPRA TOTTUS APP` de −$2.344 son exactamente los $4.507.930 del banco. Faltaba una fila de $5.000: el banco cobró **dos `COMPRA APP SBUX REWARDS CL` idénticos el 19/08** y la DB guardó uno.

**Causa raíz:** la clave de checkpoint `(fecha, descripcion, monto, num_cuotas, periodo)` **no es única** — dos compras iguales del mismo día la comparten y solo se distinguen por el `codigo_autorizacion`, que aparece recién al abrir el modal. `existing_keys` era un `set`, así que:

- en el pase normal, la primera fila se procesaba, su clave entraba al set y la segunda salía como *"Ya procesado"* **antes de abrir su modal** — nunca se supo que tenía otro código de autorización. Como `_reset_pending()` ya había borrado su versión pendiente, la transacción desaparecía;
- en el backfill, las dos filas del estado de cuenta matcheaban contra la única fila guardada y el diff daba "no falta ninguna" (se veía en el log: 105 filas "ya en la DB" contra 104 filas realmente guardadas).

**Fix (`scraper/bank_scraper.py`):** dejar de preguntar *si* la clave está y pasar a contar *cuántas* filas hay con esa clave.

- `_load_existing_keys()` devuelve un `Counter` y `seen_keys` lleva cuántas veces se vio cada clave en el pase. Se saltea solo mientras `vistas <= guardadas`: con dos filas iguales y una guardada, la segunda se procesa y su modal revela su propio código.
- `_load_period_index()` devuelve `alias_budget` (un `Counter`): cada fila guardada cubre **una** fila del estado de cuenta, así que la que sobra queda como candidata. Reemplaza el doble chequeo clave natural + alias por un solo presupuesto por período.
- No puede crear duplicados: la unicidad real la sigue imponiendo `(codigo_autorizacion, num_cuotas)`. Si las dos filas fueran la misma transacción listada dos veces, la segunda actualiza en vez de insertar; el costo es abrir un modal de más.

**Limpieza de DB:** ninguna, pero el período cerrado hay que volver a pasarlo para recuperar lo perdido:

```bash
# primero mirar qué falta, sin escribir
pipenv run python main.py --mode scraper --headless --backfill-periodo 2026-09 --backfill-dry-run
# y recién después
pipenv run python main.py --mode scraper --headless --backfill-periodo 2026-09
```

**Verificado:** el backfill de `2026-09` recuperó el segundo Starbucks con su propio `codigo_autorizacion` (360621, distinto del 360154 de la primera compra) y la reconciliación quedó en $4.510.274 contra $4.507.930 — la diferencia es exactamente la devolución de $2.344, que el banco netea en el total y `_reconcile_log` cuenta aparte. Septiembre cuadra al peso.
## 2026-09-21

### Fix: la primera cuota de una compra con interés no se registraba en el período que la cobra

**Síntoma:** la compra `COMPRA IKEA OPEN KENNEDY` (06/09/2026, $196.730 en 3 cuotas con interés, auth `771206`) no aparecía en el período abierto del dashboard. Estaba en la DB pero en `periodo = 2026-09`, y en el ciclo que cierra el 19/10 —donde el banco sí la muestra— no existía.

**Causa raíz:** la clave de checkpoint `_movement_key` era `(fecha, descripcion, monto, num_cuotas)`, **sin el período**. En las compras *sin interés* nunca molestó porque el banco avanza el número de cuota cada mes (01/03 → 02/03 → 03/03) y la clave cambia sola. En una compra **con interés** el banco repite la fila idéntica en el ciclo siguiente —misma fecha, misma descripción, mismo monto y otra vez `01/03`—, así que la clave colisionaba con la del mes anterior y `extract_all_movements` la descartaba:

```
[7/11] Ya procesado — COMPRA IKEA OPEN KENNEDY
```

Cómo factura el banco estas compras: el mes de la compra el ciclo abierto la muestra como `01/03`, pero el estado de cuenta de ese mes la lista como **`00/03`** y solo cobra el impuesto (`IMPUESTO COMPRA CUOTAS …`). La columna "Cuota a pagar" igual muestra el valor de la cuota que viene ($72.336), así que no sirve para distinguir; el "Monto facturado" del banco sí: el del 19/09/2026 ($4.507.930) no la incluye. La cuota 1 se cobra recién en el ciclo siguiente.

**Fix (`scraper/bank_scraper.py`):**
- `_movement_key()` incorpora el período (`YYYY-MM`, vía el nuevo `_periodo_de()`), y `_load_existing_keys()` / `_load_incomplete_keys()` lo traen de la DB. En el pase normal cae al del ciclo vigente; el pase de facturados lo setea por fila con el label del estado de cuenta (`_billed_collect()` ahora lo recibe), lo que además acota su chequeo de duplicados al período que está leyendo.
- `_backfill_row_ok()` descarta las filas con cuota `0/N`: el estado de cuenta las lista pero el período no las cobra. Sin esto el backfill volvía a meter la cuota en el mes viejo y quedaba duplicada.
- Guarda de paginación del estado de cuenta: una página **vacía** ahora también cuenta como lectura sospechosa de corte. Antes "limpiaba" la sospecha que dejaba la página llena anterior y el pase daba por completo un estado de cuenta leído a medias (visto en una verificación: 40 de 108 filas, reportadas como "no falta ninguna fila" / "1 fila faltante").

**Limpieza de DB:** ninguna, pero conviene revisarla si tienes una compra en cuotas **con interés**. La fila se mueve sola de período en la primera corrida con el fix (`ON CONFLICT (codigo_autorizacion, num_cuotas)` la actualiza, no la duplica) y la clasificación viaja con el `codigo_autorizacion`. Para comprobarlo:

```sql
SELECT id, periodo, periodo_facturacion, num_cuotas, monto_periodo
  FROM movimientos WHERE codigo_autorizacion = '771206';
-- debe quedar UNA fila, en el período que cobra la cuota
```

**Verificado:** corrida real — la IKEA pasa de "Ya procesado" a `(confirmado) COMPRA IKEA OPEN KENNEDY — $196.730`, las otras cuotas del ciclo siguen salteándose (no hay re-procesamiento masivo), la fila queda única en `2026-10` y septiembre baja $72.336. El backfill de `2026-09` descarta la fila `00/03` con el motivo esperado, inserta las 2 filas de la cola del ciclo que sí faltaban y la reconciliación contra el "Monto facturado" del banco pasa de una diferencia de ~$70.000 a $2.656.

**Pendiente de observación:** en el ciclo de noviembre hay que confirmar que la IKEA entra como fila **nueva** `02/03` y que la de octubre no se mueve. Si el banco repitiera `01/03` todos los meses, la identidad de estas compras dejaría de ser `(codigo_autorizacion, num_cuotas)` y habría que ampliarla con el período.
## 2026-08-20 (4)

### Fix: la lectura del estado de cuenta se cortaba a mitad y marcaba el período como completo

**Síntoma:** al recuperar los períodos viejos con `--backfill-periodo`, el dry-run de `2026-05` leyó **60 filas en 3 páginas** y concluyó *"no falta ninguna fila"*. Una segunda corrida idéntica leyó **91 filas en 5 páginas** y encontró **4 filas faltantes**. En una corrida real la primera lectura habría marcado el período como completado dejando esas 4 filas afuera para siempre — exactamente la falla que la marca de idempotencia existe para evitar.

**Causa raíz:** `_next_page_rect` busca el botón `›` y devuelve `None` si está deshabilitado o mide 0×0. Si Angular está a mitad de un update cuando se evalúa, el botón puede estar en cualquiera de esos dos estados **sin que la paginación haya terminado**. Un solo falso negativo corta la lectura. Agrava el problema que el `wait_for_function` de `_go_next_page` usa `document.querySelector`, que no atraviesa shadow DOM: siempre falla y cae en una espera fija de 2s, así que el estado de la página al momento del chequeo es una carrera.

**Fix (`scraper/bank_scraper.py`):**
- `_billed_has_next()`: reintenta el chequeo hasta 3 veces con 1,2s de espera antes de aceptar que la paginación terminó.
- **Guarda de corte**: una paginación que termina bien lo hace con una página **parcial**. Si la última página vino completa (20 filas) y aun así el botón de siguiente no aparece, se loguea un warning y **se aborta el pase devolviendo 0 filas** — que por el camino del `total == 0` no marca el período. Mejor reintentar mañana que marcar incompleto.

**Verificado:** dos lecturas consecutivas de `2026-05` dan 91 filas / 5 páginas / 4 faltantes, idénticas.

### Recuperación de los períodos con la cola cortada

_Lo que sigue son los resultados de una instalación real, a modo de ejemplo de qué esperar._

Con el fix arriba, se recuperaron los cinco períodos cerrados que tenían el final del ciclo incompleto. **19 filas en total**, todas confirmadas (`pendiente = FALSE`) y con su valor de cuota correcto:

| período | filas antes → después | max(fecha) antes → después | recuperadas |
|---|---|---|---|
| 2026-03 | 111 → 115 | 18/03 → 19/03 | 4 (3 UBER EATS + una `DEVOLUCION` de −$7.980) |
| 2026-04 | 96 → 99 | 17/04 → 18/04 | 3 |
| 2026-05 | 87 → 91 | 17/05 (sin cambio) | 4 cuotas |
| 2026-06 | 92 → 94 | 17/06 → 18/06 | 2 |
| 2026-07 | 91 → 97 | 18/07 → 19/07 | 6 |

El caso de `2026-05` es distinto de los otros: no le faltaba la cola del ciclo sino **cuatro cuotas en el medio de series por lo demás completas**. La de MP *RODACORP iba `04/12` (mar), `05/12` (abr), **hueco**, `07/12` (jun)… y ahora la serie está entera: `04/12 · 05/12 · 06/12 · 07/12 · 08/12 · 09/12 · 10/12`. Los auth codes de las filas insertadas coinciden con los de esas mismas compras en los otros períodos, y no colisionaron porque `num_cuotas` es parte de la llave única.

**Efecto secundario bueno:** las clasificaciones huérfanas por `codigo_autorizacion` bajaron de **39 a 34** — cinco categorizaciones hechas sobre movimientos que habían desaparecido volvieron a tener su fila.

**Integridad verificada** contra la línea base: los duplicados por clave natural siguen en 2 (el par ANTHROPIC preexistente de 2026-03), 0 auth codes en más de un período, 0 duplicados de `(tx_hash, periodo)`, y ningún período distinto del objetivo se movió. Todas las filas nuevas llevan `backfill_run_id`, así que son reversibles con exactitud.

**Una corrida abortó y se comportó bien:** el primer intento de `2026-04` falló con `seleccion_fallida` (el dropdown no confirmó el estado de cuenta pedido), no escribió nada y **no marcó** el período. El reintento lo completó.

**Cómo recuperar tus propios períodos:** busca los que tengan `max(fecha)` varios días antes del cierre —

```sql
SELECT periodo, count(*), min(fecha), max(fecha)
  FROM movimientos GROUP BY periodo ORDER BY periodo DESC;
```

— y ejecuta cada uno con dry-run primero, **leyendo la lista de candidatas antes de escribir**:

```bash
python main.py --mode scraper --headless --backfill-periodo 2026-05 --backfill-dry-run
python main.py --mode scraper --headless --backfill-periodo 2026-05
```

Si una ejecución se aborta (`seleccion_fallida`, o el aviso de lectura cortada), no escribió nada ni marcó el período: basta con reintentarla.

**Limpieza de datos:** no se requiere.



## 2026-08-20 (3)

### Rediseño de la vista Análisis

**Problema:** la página funcionaba pero el gráfico de abajo, "Tendencia por categoría", no aportaba. Medido, no era cuestión de gusto — fallaba en tres capas a la vez:

1. **Dibujaba hasta 16 líneas.** El techo legible para series categóricas es 7–8; pasado eso hay que plegar la cola, facetear o usar énfasis.
2. **Nueve de las 19 categorías tenían el mismo gris `#9E9E9E`** — entre ellas la 3ª y 4ª por gasto histórico. Nueve líneas del mismo color. El validador de paleta sobre las 6 categorías de mayor gasto daba `ΔE 0.0` y el veredicto *"hard to tell apart even with full color vision"*. La causa estaba en el código: `create_categoria` y el `st.color_picker` de Presupuesto arrancaban los dos en ese gris, así que toda categoría creada sin tocar el selector salía gris.
3. **El tooltip estaba roto.** `f"...<br>${{y:,.0f}}<extra></extra>"` le pasaba a plotly el texto literal `${y:,.0f}` en vez de `%{y:$,.0f}`, así que el hover nunca mostró un monto.

Y una cuarta, de honestidad: el período en curso está incompleto por definición, así que cualquier serie histórica que lo incluyera sin marcarlo terminaba en una caída inexistente.

**Cambio de fondo en la jerarquía.** El gasto **con presupuesto** es el único gestionable; el gasto sin presupuesto no se administra y parte puede volver como reembolso. La página ahora lo refleja en todos sus bloques en vez de mezclar los dos.

**El gráfico de abajo, "Gasto por período".** Reemplaza a la tendencia. Barra azul desde cero = gasto de las categorías con presupuesto; gris apilada arriba = el resto; marca horizontal = presupuesto total del período. De un golpe se ve en qué meses el gasto gestionable cruzó el tope. El período en curso va con trama y la etiqueta `· en curso`. Al seleccionar una categoría arriba, el mismo gráfico pasa a **modo énfasis**: esa categoría mes a mes **en su propia escala** (contra el total del mes su barra sería una astilla), coloreada por su estado contra su propio tope, con su presupuesto como marca en cada mes.

**El presupuesto pasa a ser marca de objetivo, no barra de fondo.** La barra fantasma gris quedaba **completamente tapada justo cuando importa**: cuando el gasto supera el tope. Ahora es una marca perpendicular, visible siempre, dentro o fuera de la barra — la lectura de un bullet chart.

**Escalas separadas.** Las barras de progreso ya no comparten eje con las categorías sin presupuesto: antes la más grande de esas ($1.914.756 en agosto) fijaba la escala y aplastaba a todas las gestionables (máx $859.604). El bloque sin presupuesto se movió a un expander propio, plegado, con su total en el título y su tabla seleccionable.

**Estado con ícono y texto, nunca solo color.** Las etiquetas pasan a `⚑ 434% · $434.421`, `⚠ 97% · $116.768`, `✓ 72% · $115.970`, con la paleta de estado fija (`#0ca30c` / `#fab219` / `#d03b3b`).

**Se elimina el color por categoría.** Con 19 categorías y 8 slots validados, codificar identidad con color nunca iba a cerrar. El color queda reservado para lo que tiene pocos valores y mucho significado — el estado contra el presupuesto — y la identidad la cargan la posición, el nombre y el detalle de una categoría a la vez. **Se saca el selector de color** del alta de categoría, que era la fuente del gris repetido. Sin migración y sin escrituras en la DB: la columna `color` queda como está y deja de tener uso visual.

**Módulo nuevo `dashboard/theme.py`** (antes no había ningún módulo compartido del dashboard): paleta validada (8 slots claro/oscuro + estado + chrome), `money()` en formato chileno (`$1.234.567` en vez de `$1,234,567`), `money_md()`, `status_of()` tolerante a NaN, `apply_layout()` y `periodo_corto()`. La paleta pasa los seis chequeos en claro y oscuro; el orden de los slots es el mecanismo de seguridad, no cosmética, y está documentado en el módulo junto al comando para re-validar.

**Tema explícito `.streamlit/config.toml`.** No existía, así que la app tomaba el tema por defecto de Streamlit, que sigue la preferencia del sistema de cada visitante. Como `.gitignore` ignoraba `.streamlit/` completo, el patrón pasó a `.streamlit/*` + `!.streamlit/config.toml` — si no, el tema nunca llegaba a Streamlit Cloud. `secrets.toml` sigue ignorado (verificado con `git add --dry-run`).

**Modo oscuro arreglado de paso.** `apply_layout()` no fija colores de fuente ni de fondo, así que los gráficos heredan la superficie del tema activo. Antes usaban el template `plotly` por defecto y salían con fondo blanco fijo, más un `rgba(200,200,200,0.35)` hardcodeado para la barra de presupuesto.

**Bugs vivos arreglados de paso:**
- **`pct` es `NaN`, no `None`.** `get_resumen_vs_presupuesto` devuelve `None` desde un `apply` y pandas lo convierte a NaN en una Series float64, así que la guarda `x is not None` era `True`: una categoría sin presupuesto pero con gasto mostraba la etiqueta **`"nan%"`** y caía en el color de "todo bien". Ahora se usa `pd.isna()` y esas categorías no van al gráfico de progreso.
- **La selección viaja por `customdata`** (el id de la categoría) y no por el texto del eje y, que se rompía con cualquier cambio de etiqueta. Se deja el nombre como respaldo.
- **`expand_splits` cacheado.** Es un loop `iterrows` que se ejecutaba en cada rerun, y la página se vuelve a ejecutar con cada clic.
- **La fila TOTAL salió de la tabla** al pie. Además de leerse mejor, el guardado la esquivaba con `edited.iloc[:-1]`, que deja de apuntar a la fila TOTAL en cuanto se ordena la tabla por otra columna.
- **Dos montos en un mismo `st.caption` se renderizaban como fórmula LaTeX**: Streamlit interpreta el par de `$` como matemática. De ahí `money_md()`, que escapa el signo.
- Tope de alto en la tabla del drill-down: una categoría con 30 movimientos estiraba la página entera.

**Verificado en la app real**, que es un paso del método y no un extra: los tres estados (sin selección, con categoría seleccionada, período en curso), en claro y en oscuro. Y contra la DB, **los números no se movieron**: total presupuestado, gastado, sin presupuesto, total del período y el desglose por categoría son idénticos en los 7 períodos. El rediseño es visual; si un número se movía, era un bug.

**Limpieza de datos:** no se requiere. Tampoco hay migración: el rediseño es de dashboard.

**Si usas esta plantilla:** revisa si tus categorías comparten color, que era el síntoma original —

```sql
SELECT color, count(*), string_agg(nombre, ', ')
  FROM categorias GROUP BY color HAVING count(*) > 1;
```

— aunque después de este cambio ya no importa visualmente: el dashboard no usa el color de la categoría en ningún gráfico. Los montos que aparecen arriba son de una instalación real, a modo de ejemplo.



## 2026-08-20 (2)

### Feature: backfill del período facturado

**Problema:** El scraper leía una sola vista, la pestaña "Últimos movimientos", que muestra el ciclo **abierto**. Cuando el ciclo cierra (el 19), las compras que todavía estaban `pendiente` desaparecen de esa pestaña — y `_reset_pending()` ya las había borrado con un `DELETE FROM movimientos WHERE pendiente = TRUE` sin filtro. Nada las volvía a insertar: **se perdían para siempre**, la cola de cada ciclo, todos los meses. En la instalación de referencia el período `2026-07` tenía 91 filas y `max(fecha) = 18/07`, contra las **97 filas** que el banco lista en el estado de cuenta del 19/07/2026.

**Fix:** un segundo pase que, cuando el ciclo cerró, entra a "Movimientos facturados", lee el estado de cuenta del período recién cerrado y completa lo que falte.

**El detalle se publica con atraso.** El estado de cuenta recién cerrado muestra `Monto facturado` pero la tabla dice *"Aún no tienes movimientos en tu tarjeta"* durante varios días. Por eso el disparador es un **reintento diario**: cada corrida mira si el detalle está publicado y **la marca de idempotencia se escribe solo si el pase leyó filas**. Sin esa regla la primera corrida post-cierre marcaría el período como hecho leyendo cero filas, y la feature perdería el mes entero todos los meses. Hay un tercer estado, distinto del vacío: `error_carga` ("No pudimos cargar tus movimientos facturados"), que se maneja usando el botón **Reintentar** del propio banco hasta 3 veces.

**Cómo funciona (`scraper/bank_scraper.py`):**
- `backfill_period()` corre **después** del pase normal, así que un fallo del backfill no cuesta el scrape del día, y `_reset_pending()` sigue corriendo una sola vez.
- Reusa el loop existente sobrescribiendo `self.row_selector`: la pestaña de facturados es otro componente (`app-invoiced-movements`) pero por dentro usa el **mismo `app-movements-table`**, así que las seis columnas caen en los mismos índices y `_read_row` sirve sin cambios. El selector se restaura en un `finally`; `navigate_to_movements` sigue esperando el selector por defecto.
- **Dos fases**: la primera pagina leyendo solo celdas de tabla (sin abrir un solo modal) y calcula el diff — eso es también el dry-run gratis. La segunda vuelve a la página 1 y abre el modal únicamente de las filas faltantes.
- Período objetivo derivado del label `Próxima facturación` **menos un mes**: el valor cambia justo cuando el banco rota el label, que *es* la definición de "el ciclo cerró". Sin aritmética de fechas ni zonas horarias.
- `--backfill-periodo YYYY-MM` fuerza un período (el banco ofrece los **últimos 12 estados de cuenta**) y `--backfill-dry-run` lista lo que falta sin escribir. También como `workflow_dispatch.inputs` y en el expander "Opciones avanzadas" de la página Scraper.

**La misma cuota se llama distinto en cada pestaña.** El hallazgo que el dry-run destapó y que ninguna guarda numérica habría atrapado: "Últimos movimientos" muestra `COMPRA CUOTAS SIN INTERES MERCADO PAGO` y "Movimientos facturados" muestra `COMPRA EN CUOTAS MERCADO PAGO` — **y la fecha difiere en un día** (DB `2025-10-06` vs facturados `07/10/2025`). La clave natural `(fecha, descripcion, monto, num_cuotas)` no puede matchearlas, así que 7 cuotas que ya estaban guardadas aparecían como "faltantes". Con 87% de solape y 13 candidatas, el pase pasaba las dos guardas numéricas y habría reescrito 7 filas correctas. Dos defensas:
- `_alias_key()` — `(descripción sin el prefijo de cuotas, monto, num_cuotas)`, acotada al período.
- **Guarda dura por auth code**: antes de escribir, si `(codigo_autorizacion, num_cuotas)` ya existe en el período, la fila no se toca. Es la llave única real de la tabla, y las filas facturadas **sí** traen auth code.

**Otras guardas:**
- Se fuerza `pendiente = False` en el pase de facturados. Un estado de cuenta no tiene pendientes; si el modal fallara, la cascada de detección marcaría pendiente, el reset del día siguiente borraría la fila y el período ya estaría marcado como hecho — pérdida silenciosa y permanente. Además `_reset_pending()` ahora excluye `backfill_run_id IS NOT NULL`.
- Solape mínimo del 50% sobre el set completo de filas: si la vista cambia de formato y las claves dejan de matchear, aborta sin escribir. Tope de 25 filas nuevas por pase.
- Se descarta (y se loguea) toda fila en cuotas sin `Cuota a pagar`: `monto_periodo` caería al monto total de la compra e inflaría el período.
- **`_upsert_to_db`: el `DELETE` del huérfano por `tx_hash` ahora está acotado por período.** Todas las cuotas de una compra comparten `tx_hash`, así que sin el filtro un backfill de un mes viejo borraría filas de los meses posteriores de la misma compra. **Esto también tapa un bug latente del pase normal**, que podía borrar la cuota de un período anterior al procesar la del siguiente con auth code recién disponible.
- La reconciliación contra `Monto facturado` es **solo log**: pagos y reversas rompen la igualdad legítimamente (un período con un pago grande puede sumar negativo).

**Migración:** aplicar `analytics/migrations/007_backfill_facturados.sql` en el SQL Editor de Supabase — agrega `scraper_runs.backfill_periodo` (marca de idempotencia) y `movimientos.backfill_run_id` + índice parcial (procedencia, para revertir con exactitud). Los dos `ALTER` son metadata-only; si el statement queda colgado no es lentitud, es un lock (ver la query de `pg_stat_activity` en `CLAUDE.md`).

**Recuperar los meses con hueco.** Si vienes usando el scraper desde antes, es muy probable que te falte la cola de cada ciclo cerrado. Verifica el hueco:

```sql
-- ¿hasta qué día llegó cada período? Un max(fecha) varios días antes del 19 es el síntoma.
SELECT periodo, count(*) filas, min(fecha) f_min, max(fecha) f_max
  FROM movimientos GROUP BY periodo ORDER BY periodo DESC;
```

Y recupéralos período por período, **con dry-run primero** y leyendo la lista de candidatas antes de escribir:

```bash
python main.py --mode scraper --headless --backfill-periodo 2026-07 --backfill-dry-run
python main.py --mode scraper --headless --backfill-periodo 2026-07
```

El banco ofrece los últimos 12 estados de cuenta, así que se puede recuperar hasta un año atrás. Después de cada corrida conviene revisar que no se movió ningún otro período:

```sql
SELECT periodo, count(*), coalesce(sum(monto_periodo),0) FROM movimientos GROUP BY 1 ORDER BY 1 DESC;
SELECT codigo_autorizacion, num_cuotas, array_agg(DISTINCT periodo)
  FROM movimientos WHERE codigo_autorizacion IS NOT NULL
 GROUP BY 1,2 HAVING count(DISTINCT periodo) > 1;   -- debe dar 0 filas
```

**Revertir un backfill:**

```sql
SELECT id, fecha, descripcion, monto, num_cuotas, codigo_autorizacion, tx_hash, periodo
  FROM movimientos WHERE backfill_run_id = :run_id ORDER BY fecha;   -- mirar primero
DELETE FROM movimientos WHERE backfill_run_id = :run_id;
UPDATE scraper_runs SET backfill_periodo = NULL WHERE id = :run_id;  -- libera la marca
```

**Sin limpieza de datos:** el pase es insert-only sobre las filas que faltan; las que ya estaban no se tocan.


## 2026-08-20

### Fix: el sitio público migró a Next.js y el login del scraper quedó roto

**Síntoma:** El scraper falla en el login, agotando los 3 intentos, con `No apareció el botón de ingreso en el header`. En GitHub Actions el job sale con exit code 1 y el screenshot `login_btn_not_found.png` muestra la home **cargada correctamente** — no es una caída del banco. Primera corrida afectada: 2026-08-18.

**Causa raíz:** El banco reconstruyó el sitio público como una app **Next.js**. Los cuatro anclajes del login desaparecieron:

| Antes | Ahora |
|---|---|
| `//div[@id='main-header__sub-content']/div[3]/button[3]` | El id `main-header__sub-content` ya no existe; el botón es `Mi Cuenta` dentro de `#main-header` |
| `input[placeholder='RUT']` | `input#document` (placeholder `Ej: 12345678-9`, `maxlength=10`, formatea el RUT solo) |
| `input[placeholder='Clave Internet']` | `input#pass` (`maxlength=6`) |
| `button#desktop-login` | El `button[type=submit]` del form del drawer (texto "Ingresar"), deshabilitado hasta que ambos campos son válidos |

Además el formulario ya no está en la página: vive en un **drawer** que monta sus inputs sólo al hacer clic en "Mi Cuenta". Las clases son CSS-modules con hash (`MiddleNav_buttonPrimary__09N_a`, `DrawerFormLogin_form-container__k1Si1`) y cambian en cada deploy, así que no sirven como selectores.

**Fix (`scraper/bank_scraper.py`):**
- `login()`: sólo anclas estables — el id `#main-header`, el texto del botón (`button:visible` + `has_text="Mi Cuenta"`), y los ids `#document` / `#pass`. Nada de clases con hash.
- **Re-click por hidratación:** Next.js sirve el header por SSR antes de montar el handler, así que el primer click puede caer al vacío. Si `#document` no aparece en 6s, se reintenta el click una vez antes de fallar.
- **RUT sin puntos:** el campo tiene `maxlength=10`, así que un RUT con puntos (12 caracteres) se truncaría al escribirlo carácter por carácter. Se manda `self.username.replace(".", "")`. **Ojo con `#pass`: tiene `maxlength=6`.** La clave internet de 6 dígitos entra justo; si tu `FALABELLA_PASSWORD` es más largo, el campo lo trunca en silencio y el login falla con este mismo síntoma.
- **Submit sin clases con hash:** se ancla con `form:has(#pass) button[type='submit']:not([disabled])`, manteniendo la espera a que se habilite.
- **No se usa `placeholder` en ningún selector:** la home tiene un simulador de crédito con un input `Ingresa tu RUT` que un selector laxo por placeholder engancharía.
- `_dismiss_service_popup()`: ahora exige que el botón `Entendido` esté **dentro** del popup de servicio (se sube por los ancestros — atravesando shadow roots, sin pasar de `<body>` — buscando el texto "no lo podemos atender"). El sitio nuevo tiene otros dos `Entendido`: el aviso de cookies y `#btn-login-client-nuevo`, **dentro del drawer de login**. Ese segundo es el riesgo real: en los reintentos de login el drawer queda abierto y la versión anterior podía hacerle clic.

**Verificado:** dos corridas locales `python main.py --mode scraper --headless` → `success`, `Login exitoso` en ~5s, sin screenshots de error. El área privada (Angular + Shadow DOM) **no cambió**: tabla, modales de detalle y extracción del período siguen funcionando sin tocar nada.

**Limpieza de datos:** no se requiere. `_reset_pending()` corre **después** del login y de `navigate_to_movements()`, así que las corridas fallidas nunca tocan `movimientos` — sólo dejan filas con `estado = error` en `scraper_runs`. Los pendientes quedan congelados en los de la última corrida exitosa y se refrescan solos en la primera corrida verde. Si quieres verificar el hueco:

```sql
-- corridas fallidas del período
SELECT id, inicio, estado, left(coalesce(error, ''), 80)
  FROM scraper_runs WHERE estado = 'error' ORDER BY id DESC LIMIT 10;

-- antigüedad de los pendientes actuales (deberían ser de la última corrida verde)
SELECT count(*), max(fecha_compra) FROM movimientos WHERE pendiente = TRUE;
```


## 2026-08-14

### Fix: modal de opt-in CMR Puntos bloqueaba el click en la tarjeta

**Síntoma:** El scraper falló en GitHub Actions los días 12, 13 y 14 de agosto de 2026 con `TimeoutError: Locator.click: Timeout 30000ms exceeded` sobre `a.div-product` (tarjeta CMR Mastercard). El call log de Playwright repetía: `<div class="backdrop"> ... subtree intercepts pointer events`. El screenshot de debug mostró un modal nuevo: _"¡Pronto podrás acumular más CMR puntos!"_.

**Causa raíz:** El banco agregó un modal de opt-in tras el login — componente Angular `app-popup-terms-conditions-optin#optin`, con `div.backdrop` (z-index 1000) a pantalla completa. El manejo de backdrop existente sólo cubría el modal de marketing antiguo (`#background-shadow.backdrop.visible` + `app-marketing button`), así que el backdrop nuevo nunca se cerraba e interceptaba todos los clicks.

**Fix (`scraper/bank_scraper.py`):**
- `_dismiss_blocking_modals()`: reemplaza el bloque específico de `#background-shadow`. Espera hasta 5s a que aparezca cualquier backdrop bloqueante (`JS_HAS_BLOCKING_BACKDROP` verifica visibilidad real de `div.backdrop, .cdk-overlay-backdrop`), lo cierra y **verifica que desapareció**, hasta 3 intentos. Si queda bloqueado, guarda screenshot `backdrop_bloqueado.png`.
- Selectores de cierre en orden: `#optin button.close-misdocumentos` → `app-popup-terms-conditions-optin button[class*='close']` → `app-marketing button` → fallback JS `JS_CLICK_PROMO_CLOSE` (busca botones de cierre por clase/aria/`svg.icon-close`) → Escape.
- **El modal se cierra siempre con la ×, nunca con "Acepto"**: ese botón inscribe al usuario en el programa CMR Puntos y autoriza comunicaciones comerciales. `JS_CLICK_PROMO_CLOSE` sólo matchea botones de cierre y excluye explícitamente "Cerrar sesión".
- Click en la tarjeta con reintento: si falla, vuelve a cerrar modales y reintenta; como último recurso `card_link.evaluate("el => el.click()")` (click directo en el DOM, ignora el overlay).

**Verificado:** `pipenv run python main.py --mode scraper --limit 1 --headless --debug` → `Modal bloqueante cerrado (#optin button.close-misdocumentos)`, tabla cargada, 5 páginas recorridas.

**Limpieza de datos:** no se requiere — los runs fallaron antes de escribir en la base.

### Doc: el banco volvió a servir `Código autorización`, `Pais` y `Origen de la compra`

`CLAUDE.md` decía que estos tres campos quedaron eliminados del modal en ~2026-03 y son "always NULL". Es falso: el bache fue **abril 2026** y el banco los volvió a servir **~2026-05-08**. Cobertura de `codigo_autorizacion` medida en una cuenta real, por mes de transacción: 04/2026 33/87, 05/2026 57/89, 06/2026 86/90, 07/2026 79/85, 08/2026 42/43 (~95% desde junio). El código nunca dejó de extraerlos (`JS_EXTRACT_FIELDS.LABELS` los mantiene), así que solo la documentación estaba desfasada.

- `CLAUDE.md`: corregidas las notas de las tres columnas; la sección **Installment uniqueness** ahora describe los dos caminos (auth code / `tx_hash`) como **estado permanente**, no como una migración en curso, e indica cuál aplica a cada fila (lo que el modal haya servido, pendientes incluidos: hoy la mayoría de los pendientes trae auth code y va por el camino de auth, con `tx_hash` NULL). Agregada la sección **Pendiente → confirmado key migration**, que documenta que `_save_movement` migra `clasificaciones` y `splits` de `tx_hash` a `codigo_autorizacion` en la misma transacción — relevante solo para pendientes guardados sin auth code.
- `CLAUDE.md`, **Single table**: la detección de pendiente estaba documentada como una sola regla (`modal_cuotas` ausente = pendiente). En realidad el scraper maneja tres estructuras y la vigente es la de auth code, donde el discriminador es la **ausencia de fecha en la tabla** y los pendientes **sí** traen cuotas (`01/01`) — verificado en la base: el pendiente actual tiene `fecha` NULL, `num_cuotas` `01/01` y auth code `156317`. También corregida la explicación de por qué los pendientes se re-procesan en cada run: no es por no tener cuotas, es porque `_load_existing_keys` solo carga filas `pendiente = FALSE`.
- `scraper/bank_scraper.py`: corregido el comentario y docstring de `_load_incomplete_keys`, que justificaban no reintentar por `codigo_autorizacion IS NULL` con una premisa ya falsa. El comportamiento **no cambia**: el motivo real es que las filas del bache abr–may nunca podrán obtener auth code y se re-procesarían en cada run.

**Limpieza de datos:** no se requiere para este cambio. Si vienes del bache abr–may 2026 puedes tener clasificaciones huérfanas (creadas por `tx_hash` o por un `codigo_autorizacion` que ya no existe en `movimientos`). Para revisarlas:

```sql
-- clasificaciones que no enlazan con ningún movimiento
SELECT * FROM clasificaciones c
 WHERE (c.tx_hash IS NOT NULL AND NOT EXISTS (
         SELECT 1 FROM movimientos m WHERE m.tx_hash = c.tx_hash))
    OR (c.codigo_autorizacion IS NOT NULL AND NOT EXISTS (
         SELECT 1 FROM movimientos m WHERE m.codigo_autorizacion = c.codigo_autorizacion));
```

Revisa el SELECT antes de borrar: una huérfana por `tx_hash` puede ser el duplicado benigno de una clasificación que ya migró a `codigo_autorizacion`, y borrarla no pierde información, pero las demás implican re-clasificar el movimiento a mano.

### Seguridad: activar RLS en todas las tablas (alerta `rls_disabled_in_public` de Supabase)

**Síntoma:** el linter de Supabase reporta "Table publicly accessible — anyone with your project URL can read, edit and delete all data in this table because Row-Level Security is not enabled".

**Diagnóstico:** la alerta exagera al decir que basta la URL del proyecto (también hace falta la anon key, y este proyecto no usa la API REST ni `supabase-js`). Pero la superficie es real:
- El endpoint REST está activo: `GET /rest/v1/movimientos` responde `401 No API key found`.
- RLS desactivado en las 7 tablas, 0 políticas.
- `anon` y `authenticated` tienen **todos** los privilegios en las 7 tablas, incluidos `DELETE` y `TRUNCATE`.

O sea: sin evidencia de exposición, pero a una filtración de la anon key de que alguien lea, modifique o borre todo.

**Fix (`analytics/migrations/006_enable_rls.sql`):** `ENABLE ROW LEVEL SECURITY` en las 7 tablas, **sin políticas** — RLS deniega por defecto, así que los roles de la API quedan sin acceso a ninguna fila. La aplicación no se ve afectada porque conecta con el rol `postgres`, que tiene `rolbypassrls = TRUE`. `analytics/schema.sql` actualizado al estado final.

**Si usas esta plantilla, tu proyecto tiene la misma alerta.** Antes de ejecutar la migración, verifica con qué rol conecta tu app:

```sql
SELECT current_user, rolbypassrls FROM pg_roles WHERE rolname = current_user;
```

Si da `rolbypassrls = false`, crea primero una política para ese rol o dejarás la aplicación sin datos. Después de aplicar, confirma el estado:

```sql
SELECT relname, relrowsecurity FROM pg_class
 WHERE relname IN ('movimientos','categorias','clasificaciones','splits',
                   'presupuestos','reglas_sugerencia','scraper_runs');
SELECT count(*) AS politicas FROM pg_policies WHERE schemaname = 'public';
```

**El método importa para el próximo DDL.** Hacerlo con `autocommit = True` y `SET lock_timeout` de sesión **se cuelga**: el `SET` no protege al `ALTER`, que queda esperando el lock y encola todo lo demás (en la instalación de referencia abortó la corrida del scraper que estaba en curso). Lo que funciona es una **transacción explícita** con `SET LOCAL lock_timeout = '5s'` y los 7 `ALTER` dentro: atómico y con abort rápido si algún lock no está disponible. Con las transacciones liberadas (ver la entrada siguiente) los 7 `ALTER` corren en ~1,4 s.

**Endurecimientos opcionales (no aplicados):** `REVOKE ALL ... FROM anon, authenticated` como segunda capa — los default privileges de Supabase vuelven a otorgar permisos en tablas nuevas, así que RLS es la capa durable — y desactivar la Data API o quitar `public` de los schemas expuestos, dado que el proyecto no la usa.

**Limpieza de datos:** no se requiere, el cambio solo afecta permisos.

### Fix: las lecturas dejaban transacciones abiertas y bloqueaban todo el DDL

**Síntoma:** aplicar `006_enable_rls.sql` en el SQL Editor fallaba con `Error: SQL query ran into an upstream timeout`. Reintentar desde psycopg2 con `lock_timeout = '4s'` falló en las 7 tablas con `LockNotAvailable`.

**Causa raíz:** no era lentitud ni el pooler. `pg_stat_activity` mostraba dos sesiones del dashboard (`application_name=Supavisor`) en estado **`idle in transaction`** desde más de media hora, sosteniendo `AccessShareLock` sobre tres tablas. `ALTER TABLE` necesita ACCESS EXCLUSIVE, así que quedaba en cola detrás de ellas hasta el timeout.

El origen: psycopg2 abre una transacción implícita en el primer `execute`. Las funciones de solo lectura cerraban el cursor pero nunca hacían commit ni rollback, así que la transacción quedaba abierta. Como el dashboard cachea la conexión con `@st.cache_resource`, esa transacción vivía lo que vivía la app. Efecto secundario: autovacuum tampoco podía limpiar esas tablas.

**Fix:**
- `analytics/db.py`: nuevo context manager `read_cursor(conn)` — cierra el cursor y hace `rollback()` al salir, terminando la transacción implícita. **No** se usó `autocommit = True` a nivel de conexión a propósito: rompería la atomicidad de operaciones como reclasificar un split (borrar el split + insertar la clasificación bajo un solo commit).
- Convertidas las 11 funciones de solo lectura: `analytics/loader.py` (3), `analytics/repository.py` (`get_categorias`, `get_presupuesto`, `get_presupuestos_periodo`, `get_clasificacion`, `get_splits`), `analytics/classifier.py` (`sugerir_categoria`, `aplicar_seed_desde_rubro`) y `dashboard/pages/04_Scraper.py` (`load_runs`). Las que escriben quedan intactas con su commit explícito.
- `CLAUDE.md`: la nota de migraciones atribuía el timeout del DDL al session pooler. Era incorrecto; ahora explica que es un problema de locks, con la consulta de `pg_stat_activity` para diagnosticarlo.

**Cómo detectarlo en tu instalación:**

```sql
SELECT pid, usename, application_name, state, now() - xact_start AS tx_abierta, left(query, 60)
  FROM pg_stat_activity
 WHERE datname = current_database() AND state = 'idle in transaction';
```

Si aparecen sesiones ahí, un redeploy del dashboard las libera — pero el fix de raíz es usar `read_cursor()` en toda ruta de lectura.

**Limpieza de datos:** no se requiere.

## 2026-05-25

### Fix: reintentos automáticos de login + escritura humana de credenciales

**Síntoma:** El scraper fallaba ~3 de cada 10 runs en GitHub Actions con "Login fallido". El patrón en los logs muestra el popup "En estos momentos no lo podemos atender" **dos veces**: una al cargar la página (normal, ya manejada) y otra 20 segundos después, coincidiendo exactamente con el timeout de `wait_for_selector("Hola", 20000)`.

**Causa raíz:** El banco detecta la automatización durante el envío de credenciales y muestra el popup de "servicio no disponible" a nivel de respuesta del backend. Descartar el popup no rescata la sesión ya rechazada. El bloqueo es intermitente (probabilístico) — algunos días bloquea todo el día (23/05), otros días funciona sin problemas.

**Fix (`scraper/bank_scraper.py`):**
- `_login_with_retry()`: wrapper que reintenta `login()` hasta 3 veces con 30s de espera entre intentos. Cada intento recarga la página desde cero (`page.goto()` al inicio de `login()`).
- `page.wait_for_timeout(1500)` tras `domcontentloaded`: pequeña espera para que Angular termine de renderizar el header antes del popup check.
- `rut_field.type(username, delay=80)` / `pass_field.type(password, delay=80)` en vez de `fill()`: genera key events por carácter (~80ms entre teclas) imitando escritura humana, reduciendo la señal de automatización.
- `run()` usa `_login_with_retry()` en vez de `login()` directamente; mensaje de error actualizado a "Login fallido tras 3 intentos".

**Limitación conocida:** Si el banco bloquea el IP de GitHub Actions durante todo el día (como ocurrió el 23/05), los 3 reintentos tampoco alcanzan. En ese caso la solución definitiva es correr desde un IP fijo o variar el horario del cron.

## 2026-05-14

### Fix: robustez del login en GitHub Actions — detección headless, popup Shadow DOM, exit code y diagnóstico

**Síntoma:** El scraper en GitHub Actions fallaba con `Login fallido` o timeout en `networkidle`. Localmente funcionaba en modo normal pero no en headless. Los runs fallidos aparecían como "success" en Actions.

**Causa raíz (múltiple):**
1. `wait_until="networkidle"` nunca disparaba: el SPA del banco mantiene conexiones persistentes (polling/WebSocket) que impiden que la red quede idle.
2. El banco detectaba Chromium en modo headless (via `navigator.webdriver` y la ausencia de UA real) y mostraba un popup de bloqueo: _"En estos momentos no lo podemos atender, por favor intenta más tarde"_ con botón "Entendido".
3. El popup está dentro del Shadow DOM del banco — `page.locator()` no puede alcanzarlo; requiere JS con traversal recursivo de shadow roots.
4. El click al botón de login se hacía sin esperar que el DOM de Angular terminara de renderizarlo.
5. Los screenshots de diagnóstico solo se guardaban con `--debug`, que no se usa en Actions.
6. El scraper terminaba con exit code 0 incluso en error → Actions marcaba el run como "success".

**Fix (`scraper/bank_scraper.py`):**
- `page.goto(..., wait_until="domcontentloaded")` en vez de `networkidle`.
- `p.chromium.launch(args=["--disable-blink-features=AutomationControlled"])` + user-agent de Chrome real + `add_init_script` para anular `navigator.webdriver`.
- `_dismiss_service_popup()`: usa `wait_for_function` + `page.evaluate()` con traversal recursivo de shadow roots para encontrar y hacer clic en "Entendido". Se llama al cargar la página y tras enviar credenciales si "Hola" no aparece.
- `wait_for_selector` en el botón de login antes de hacerle clic.
- `_screenshot(..., error=True)`: nuevo parámetro que guarda el screenshot siempre (sin `--debug`) en puntos de falla. Aplicado en todos los errores de login, carga de tabla y excepción inesperada en el run.
- `_finish_run` guarda el status en `self._run_status`; `main()` llama `sys.exit(1)` si es `"error"`.

**Fix (`.github/workflows/scraper.yml`):**
- Paso `Upload debug screenshots` con `actions/upload-artifact@v4` y `if: always()` — los screenshots quedan disponibles en cada run de Actions para diagnóstico.

## 2026-05-13

### Fix: soporte dual-estructura UI banco + logging silenciado + migración de clasificaciones

**Síntoma:** El banco Falabella revirtió su frontend a la estructura anterior (~2026-03), causando tres problemas simultáneos:
1. El scraper sólo procesaba la primera página (paginación rota).
2. Movimientos pendientes eran guardados como confirmados (`pendiente=FALSE`).
3. Las clasificaciones se perdían al pasar un movimiento de pendiente a confirmado.
4. Ningún log era visible en las ejecuciones de GitHub Actions (logging completamente silenciado).

**Causa raíz:**
- `JS_NEXT_PAGE_RECT` sólo buscaba `btn-move` (nueva estructura); la antigua usa `btn-pagination`.
- La detección de pendiente usaba únicamente `modal_cuotas`, que no existe en la estructura antigua. En estructura antigua, tanto confirmados como pendientes tienen `Código autorización` en el modal; el único discriminador confiable es la presencia de fecha en la fila de la tabla.
- Al confirmar una transacción, `clasificaciones` y `splits` no se migraban si cambiaba la llave (`tx_hash → codigo_autorizacion` o `tx_hash → tx_hash` distinto).
- `logging.getLogger("root")` retorna el logger raíz de Python (porque `root.name == 'root'`), por lo que `setLevel(CRITICAL)` silenciaba todo el output.

**Fix (`scraper/bank_scraper.py`):**
- `JS_NEXT_PAGE_RECT`: busca `btn-move` (nueva) y `btn-pagination` (antigua) en paralelo.
- `JS_EXTRACT_FIELDS`: restaura labels `Código autorización`, `Pais`/`País`, `Origen de la compra` como opcionales.
- `wait_for_function`: acepta `Comercio` OR `Código autorización` como señal de carga del modal.
- `_read_row`: restaura `DATE_RE` como fallback para detectar pendientes en estructura antigua (sin fecha).
- Detección de pendiente multi-señal en `extract_all_movements`:
  - Si modal tiene `Código autorización` → estructura antigua → usar DATE_RE (sin fecha = pendiente).
  - Si modal cargó sin auth → estructura nueva → usar `modal_cuotas`.
  - Si modal no cargó → DATE_RE como fallback.
- `_save_pending_hashes()`: guarda `{(desc_norm, monto_norm): tx_hash}` de todos los pendientes antes de `_reset_pending()`.
- `_upsert_to_db`: al confirmar, migra `clasificaciones` y `splits` del hash pendiente al nuevo identificador (ya sea nuevo `tx_hash` o `codigo_autorizacion`), en la misma transacción DB. `NOT EXISTS` previene sobrescribir clasificaciones existentes.
- Logging: reemplaza `getLogger("root").setLevel(CRITICAL)` por silenciar sólo loggers específicos (`urllib3`, `asyncio`, `playwright`).

## 2026-04-27

### Feat: rediseño UX de splits en Clasificación

**Problema:** El panel "✂ Dividir movimiento" usaba un `selectbox` que defaulteaba siempre al primer movimiento de la lista. Al abrir el expander sin revisar la selección era fácil guardar un split accidentalmente sobre la transacción equivocada.

**Solución (`dashboard/pages/01_Clasificacion.py`):**
- La tabla principal cambia de `st.data_editor` (con edición inline) a `st.dataframe` con `selection_mode="single-row"` y `on_select="rerun"`.
- Al hacer clic en una fila aparece un panel de acción debajo de la tabla con el contexto de esa transacción y dos tabs: **Clasificar** y **✂ Dividir**.
- La tab "Dividir" solo es accesible tras seleccionar explícitamente una fila, eliminando el riesgo de split accidental.
- La clasificación pasa de edición inline masiva a un selectbox por fila con botón "Guardar clasificación" / "Quitar clasificación".

## 2026-04-25

### Fix: adaptación a cambios de UI del Banco Falabella (~2026-03)

**Síntoma:** Movimientos confirmados quedaban sin `codigo_autorizacion`, `pais` y `origen`. Movimientos pendientes no se detectaban correctamente. El scraper dejó de navegar a la siguiente página.

**Causa raíz:** El banco realizó cambios en su frontend Angular:
1. Eliminó `Código autorización`, `País` y `Origen de la compra` del modal de detalle.
2. Unificó la tabla de movimientos: pendientes y confirmados aparecen en la misma tabla, todos con fecha. El ícono de reloj ya no es el señal de pendiente.
3. Cambió la clase CSS de los botones de paginación de `btn-pagination` a `btn-move`.

La detección de movimiento pendiente (antes: ausencia de fecha en la tabla) quedó rota — todos los rows parecían confirmados. Y `JS_NEXT_PAGE_RECT` no encontraba botones de paginación al buscar la clase antigua.

**Fix (`scraper/bank_scraper.py`):**
- `JS_EXTRACT_FIELDS`: elimina labels `Código autorización`/`País`/`Origen`; agrega `Cuotas → modal_cuotas`. La presencia de `Cuotas` en el modal indica movimiento confirmado; su ausencia indica pendiente.
- `wait_for_function`: espera `Comercio` (8 s) en lugar de `Código autorización` (15 s).
- `_read_row`: siempre retorna `pendiente=False` con fecha real; la señal definitiva viene del modal.
- `extract_all_movements`: skip check usa `num_cuotas` truthy (no `fecha`); pendiente detectado via `detail.pop("modal_cuotas")`.
- `_load_incomplete_keys`: ya no reintenta por `codigo_autorizacion IS NULL`; solo por `rubro`/`comercio` faltante.
- `JS_NEXT_PAGE_RECT`: busca clase `btn-move` en lugar de `btn-pagination`.
- Código muerto eliminado: `DATE_RE`, `num_cuotas_raw`.

**Fix (`analytics/schema.sql` + migración `005_tx_hash_cuotas_unique.sql`):**
El constraint `UNIQUE(tx_hash)` causaba que las cuotas de una misma compra se sobreescribieran (todas comparten el mismo `tx_hash = sha256(fecha_compra|descripcion|monto)`). Se reemplaza por un índice parcial `UNIQUE(tx_hash, periodo) WHERE tx_hash IS NOT NULL` — dentro de un período hay exactamente una cuota por compra.

```sql
-- Aplicar en Supabase SQL Editor (no funciona vía pooler por statement_timeout)
ALTER TABLE movimientos DROP CONSTRAINT IF EXISTS movimientos_tx_hash_key;

CREATE UNIQUE INDEX IF NOT EXISTS movimientos_tx_hash_periodo_idx
    ON movimientos (tx_hash, periodo)
    WHERE tx_hash IS NOT NULL;
```

**Impacto en clasificaciones existentes:** Movimientos que tenían `codigo_autorizacion` antes del cambio del banco (~2026-03) y estaban clasificados quedarán con su clasificación huérfana — el banco ya no sirve el auth code, por lo que el movimiento ahora tiene `tx_hash` como identificador y no hay forma de recuperar el vínculo automáticamente. Esos movimientos deben re-clasificarse manualmente.

```sql
-- Verificar clasificaciones huérfanas (no matchean ningún movimiento actual)
SELECT c.id, c.codigo_autorizacion, c.tx_hash, cat.nombre
FROM clasificaciones c
LEFT JOIN categorias cat ON cat.id = c.categoria_id
WHERE NOT EXISTS (
    SELECT 1 FROM movimientos m WHERE
        (c.codigo_autorizacion IS NOT NULL AND c.codigo_autorizacion = m.codigo_autorizacion)
        OR (c.tx_hash IS NOT NULL AND c.tx_hash = m.tx_hash)
);
```

## 2026-04-10

### Fix: duplicados en `movimientos` al re-procesar transacciones incompletas

**Síntoma:** Transacciones aparecían duplicadas en el dashboard (misma fecha, comercio y monto, dos filas).

**Causa raíz:** Cuando el modal de detalle no cargaba durante un scrape (timing/red), la transacción se guardaba sin `codigo_autorizacion`, usando `tx_hash` como identificador de fallback. En el siguiente run, la fila quedaba en `incomplete_keys` y se re-procesaba. Si en esa segunda pasada el modal sí cargaba y entregaba el auth code, `_upsert_to_db` forzaba `tx_hash = None` (comportamiento correcto para filas con auth code), pero el bloque de limpieza previo al INSERT solo corría `if tx_hash:`, que nunca era verdadero en ese branch. El `INSERT ON CONFLICT (codigo_autorizacion, num_cuotas)` no encontraba conflicto con la fila antigua (que tenía `codigo_autorizacion IS NULL`), por lo que creaba una segunda fila.

**Fix (`scraper/bank_scraper.py`):** Se separa el cálculo del hash en dos variables:
- `potential_hash` — calculado siempre para transacciones confirmadas, usado exclusivamente para el DELETE previo.
- `tx_hash` — el valor que se persiste en la DB (sigue siendo `None` cuando hay `codigo_autorizacion`).

El DELETE ahora usa `potential_hash` en lugar de `tx_hash`, eliminando correctamente la fila antigua antes de insertar la nueva con auth code.

**Limpieza de duplicados existentes:** Si ya hay duplicados en la DB, ejecutar en el SQL Editor de Supabase:

```sql
-- 1. Verificar duplicados
SELECT old.id, old.fecha, old.descripcion, old.monto, old.tx_hash,
       new.id AS id_con_auth, new.codigo_autorizacion
FROM movimientos old
JOIN movimientos new ON (
    old.fecha = new.fecha
    AND old.descripcion = new.descripcion
    AND old.monto = new.monto
    AND COALESCE(old.num_cuotas, '') = COALESCE(new.num_cuotas, '')
    AND old.codigo_autorizacion IS NULL
    AND old.tx_hash IS NOT NULL
    AND new.codigo_autorizacion IS NOT NULL
    AND old.pendiente = FALSE
    AND new.pendiente = FALSE
);

-- 2. Eliminar las filas antiguas (sin auth code)
DELETE FROM movimientos
WHERE id IN (
  SELECT old.id
  FROM movimientos old
  JOIN movimientos new ON (
      old.fecha = new.fecha
      AND old.descripcion = new.descripcion
      AND old.monto = new.monto
      AND COALESCE(old.num_cuotas, '') = COALESCE(new.num_cuotas, '')
      AND old.codigo_autorizacion IS NULL
      AND old.tx_hash IS NOT NULL
      AND new.codigo_autorizacion IS NOT NULL
      AND old.pendiente = FALSE
      AND new.pendiente = FALSE
  )
);
```

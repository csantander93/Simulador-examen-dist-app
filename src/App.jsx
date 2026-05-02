import { useState, useCallback, useEffect } from "react";

const KNOWLEDGE_BASE = `
SISTEMAS DISTRIBUIDOS - BASE DE CONOCIMIENTO COMPLETA

DEFINICIÓN: Un sistema distribuido es aquel en el que los componentes de hardware y software se encuentran en una red de computadoras, comunicando y coordinando sus acciones solo mediante mensajes. (Colouris). Tanenbaum: colección de computadoras independientes que se presentan al usuario como un único sistema, donde el middleware oculta los detalles locales de cada componente.

CARACTERÍSTICAS DE SISTEMAS DISTRIBUIDOS (Colouris) - 6 características:
1. COMPARTICIÓN DE RECURSOS: Hardware (discos, impresoras) y software (archivos, BD) encapsulados en máquinas, accedidos vía red. Gestores de recursos administran el acceso confiable y consistente. Requieren: esquema de nombres, traducción de nombres a direcciones, coordinación de accesos concurrentes. Dos modelos: cliente-servidor y basado en objetos.
2. APERTURA (Openness): Un sistema es abierto si puede extenderse en hardware (periféricos) o software (servicios, protocolos) sin afectar lo existente. Sus interfaces clave están especificadas y disponibles públicamente. Ofrece mecanismo uniforme de comunicación. Permite hardware/software heterogéneo de distintos proveedores. Garantiza: extensibilidad, interoperabilidad, facilidad de integración.
3. CONCURRENCIA: Ejecución simultánea de múltiples procesos. Con un procesador se entrelaza; con varios se ejecuta en paralelo. Ocurre por: (1) usuarios interactuando simultáneamente en estaciones separadas, (2) procesos servidores respondiendo peticiones concurrentes. Requiere sincronización cuidadosa para evitar conflictos. Desafíos: deadlocks, comunicación no confiable.
4. ESCALABILIDAD: Debe funcionar eficientemente a cualquier escala, desde dos estaciones hasta miles de computadoras. El software no debería requerir cambios al crecer. Técnicas: replicación de datos, caching, múltiples servidores en paralelo.
5. TOLERANCIA A FALLOS: El sistema sigue funcionando ante errores. Dos enfoques: (1) redundancia hardware: componentes replicados que reemplazan a los fallidos; (2) recuperación de software: roll-back del estado de datos. Alta disponibilidad: medida del tiempo que el sistema permanece utilizable. Si un componente falla, el usuario puede cambiar de estación; un servidor puede reiniciarse en otra máquina.
6. TRANSPARENCIA: Ocultar al usuario y programador la separación física de los componentes. 8 tipos ISO 1996:
   - Acceso: usar recursos remotos igual que locales.
   - Localización: acceder sin conocer la ubicación.
   - Concurrencia: varios procesos comparten recursos sin interferencias.
   - Replicación: usar múltiples copias sin que el usuario lo note.
   - Fallos: continuar tareas pese a errores de hardware/software.
   - Migración: mover recursos sin afectar a usuarios.
   - Prestaciones: reconfigurar para mejorar rendimiento.
   - Escalado: expandir sin modificar estructura ni algoritmos.
   Las más críticas: transparencia de acceso y localización (transparencias de red). Proveen anonimato similar al de sistemas centralizados.

OBJETIVOS DE LOS SISTEMAS DISTRIBUIDOS: Apertura, Escalabilidad, Accesibilidad (garantiza acceso permanente a recursos para todos los usuarios), Transparencia.
DESAFÍOS: Concurrencia (deadlocks), Seguridad (autenticación, permisos, auditoría), Heterogeneidad (distintas redes, arquitecturas, SO, lenguajes).
CONSECUENCIAS: Ausencia de reloj global (latencia), Ausencia de contexto global (sin estado global único), Nuevos modos de falla (fallos individuales no detectados).

MIDDLEWARE: Software que facilita la comunicación entre clientes y servidores. Proporciona acceso transparente a servicios y recursos no locales. Define: API para solicitar servicios, transmisión de peticiones, devolución de resultados.
Tipos: genérico (RPC, autenticación, archivos distribuidos, sincronización de tiempo) y específico de dominio (ODBC para bases de datos).
Protocolo más usado: TCP/IP. Ejemplos: ODBC, HTTP/SSL, CORBA, DCOM, Java RMI.
Funciones: facilitar comunicación, acceso eficiente a información, seguridad, optimizar desarrollo, enlazar aplicaciones, independizar entornos.

RPC (Remote Procedure Call): Mecanismo sincrónico, invoca procedimientos remotos como si fueran locales. Paradigma cliente/servidor, sin memoria compartida, parámetros por valor.
Tipos: texto (soporta heterogeneidad, no requiere protocolo especial) y binario (asume homogeneidad, más veloz).
Usa IDL (Interface Definition Language). Funciones principales: (1) procesamiento de interfaces: marshalling/unmarshalling, despacho; (2) gestión de comunicaciones; (3) enlazado/binding: localizar servidor.
Componentes: stub del cliente (empaqueta args, convierte llamada local en remota, envía y espera) y skeleton/stub del servidor (recibe, desempaqueta, ejecuta).
Semánticas de ejecución: idempotente (puede repetirse), exactamente una vez (si falla no se ejecuta), a lo sumo una vez (reintentos hasta ejecución única).
Binder: servicio de nombres, tabla de correspondencias nombre↔puerto. Importar=pedir puerto; exportar=registrar.
Al compilar genera: componente servidor, componente cliente, mecanismo de presentación (marshalling), funcionalidad.

MOM (Message Oriented Middleware): Comunicación asíncrona mediante mensajes encolados. No requiere disponibilidad simultánea de ambos componentes. Oculta detalles de la red y arquitectura. Operaciones explícitas de envío y recepción, sin memoria compartida.
Patrones: punto a punto, multicast (un emisor → varios receptores), broadcast (un emisor → todos).
Modalidades: síncrona (emisor espera), asíncrona (emisor continúa), con buffer (cola), sin buffer (solo si receptor disponible).
Persistencia: mensajes persistentes sobreviven fallos; no persistentes pueden perderse.
Ventajas: desacopla emisor y receptor, abstrae red, facilita interoperabilidad, flexibilidad. Ejemplos: Apache Kafka.
Metáfora: como oficina postal (vs RPC como teléfono).

SOCKETS: Canal nivel transporte (TCP). Bajo nivel. El programador maneja: representación de datos, manejo de errores, asociación de componentes (conocer IPs). Pasos: socket(), connect(), accept(), read/write(), close().

APACHE KAFKA: Plataforma distribuida de mensajería orientada a eventos. Publica, almacena y consume flujos de datos en tiempo real. Alta velocidad, tolerancia a fallos, escalabilidad. Conceptos: Topic (canal lógico con particiones), Producer (publica mensajes), Consumer (lee mensajes, puede ser parte de consumer group), Broker (servidor que almacena y distribuye), Partición (log ordenado con offset), Cluster (conjunto de brokers).

MODELO CLIENTE-SERVIDOR:
Principios: Separación cliente/servidor. Autorización en servidor. Comunicación mediante protocolos. Petición-respuesta. El cliente siempre inicia.
Beneficios: Mayor seguridad (datos en servidor), acceso centralizado, facilidad de mantenimiento (servidor actualizable sin afectar clientes), interoperabilidad multiplataforma, modularidad, escalabilidad.
Servidores pesados (fat): más lógica en servidor, mayor compatibilidad, más fácil de explotar.
Clientes pesados (fat): más lógica en cliente, fácil implementación, personalización de front-end, reduce encapsulación.

SISTEMAS N-TIERED:
Two-tier: cliente y servidor, lógica puede estar en cualquiera o compartida.
Three-tier: presentación (HTML/CSS), lógica de aplicación (capa media), datos (servidor). Mayor modularidad, escalabilidad, mantenibilidad.

ARQUITECTURA BASADA EN COMPONENTES:
Principios: Reutilizable (para múltiples usos), Reemplazable (sin afectar el sistema), Encapsulado (oculta detalles internos), Independiente (mínimas dependencias), Extensible.
Beneficios: Facilidad de despliegue/reemplazo, reducción de costos (componentes de terceros), mitigar complejidad técnica, reutilización.
Casos de uso: sistemas modulares, aplicaciones empresariales con frameworks.

DISEÑO IMPULSADO POR DOMINIO (DDD):
Principios: Modelo de dominio expresivo, lenguaje ubicuo compartido entre técnicos y negocio, modularidad y flexibilidad.
Beneficios: Comunicación efectiva (lenguaje común), extensible (modular), comprobable (bajo acoplamiento, testable).
Casos de uso: sistemas complejos con dominios empresariales ricos: finanzas, logística, comercio electrónico.

ARQUITECTURA EN CAPAS:
Principios: Abstracción (simplifica visión del sistema), Encapsulación (oculta detalles), Alta cohesión (funciones relacionadas en capa), Acoplamiento flexible (comunicación por interfaces). Separar responsabilidades con mínima superposición.
Beneficios: Abstracción (modificar capas sin afectar otras), aislamiento (cambios tecnológicos locales), manejabilidad (código organizado), rendimiento (distribución en niveles físicos), capacidad de prueba (interfaces claras, mocks).
Reglas: comunicación explícita entre capas, usar abstracciones (interfaces, clases base, fachadas), no mezclar tipos de componentes en una capa, formatos de datos consistentes.
Casos de uso: aplicaciones web MVC, sistemas con separación clara presentación/lógica/datos.

BUS DE MENSAJES:
Principios: Comunicación basada en mensajes, esquemas y contratos compartidos, procesamiento modular.
Beneficios: Extensibilidad (fácil agregar/quitar aplicaciones), baja complejidad (solo conectarse al bus), flexibilidad (cambios via configuración), escalabilidad (múltiples instancias), bajo acoplamiento, simplicidad.
Casos de uso: sistemas distribuidos con alta integración, microservicios, mensajería empresarial, sistemas con muchas aplicaciones que interoperan.

ARQUITECTURA 3 NIVELES (N-Tier):
Principios: Independencia de niveles, comunicación asíncrona, descomposición funcional clara.
Beneficios: Mantenibilidad (cambios en un nivel no afectan otros), escalabilidad (niveles independientes), flexibilidad (gestión independiente), disponibilidad (tolerancia a fallos por modularidad).
Casos de uso: aplicaciones web financieras, sistemas con alta seguridad.

ORIENTADA A OBJETOS:
Principios: Abstracción, Encapsulación, Polimorfismo (comportamientos intercambiables), Desacoplamiento (interfaces abstractas).
Beneficios: Comprensible (modela mundo real), reutilizable, probable/testable (encapsulación facilita pruebas), extensible (cambios en datos no afectan interfaces).
En ambiente distribuido: debe ser transaccional, seguro, lockeable, persistente.

ORIENTADA AL SERVICIO (SOA):
Principios: Servicios autónomos (independientes en desarrollo y despliegue), contratos compartidos (esquemas no clases), acoplamiento flexible (interfaces estándar), servicios distribuibles, compatibilidad basada en políticas.
Beneficios: Alineación de dominios (reutilización reduce costos), abstracción (contratos ocultan implementación), interoperabilidad (estándares multiplataforma), capacidad de descubrimiento (servicios publican descripciones).
Conceptos: proveedor (ofrece capacidades), consumidor (busca satisfacer necesidad), capacidad (tarea que ofrece el proveedor), servicio (mecanismo de acceso a capacidades), descripción del servicio.
Casos de uso: sistemas empresariales distribuidos, integraciones entre plataformas heterogéneas (ERP, CRM), autenticación federada.

WEB SERVICES: Aplicación multiplataforma y distribuida que puede describirse, publicarse, localizarse e invocarse a través de una red. Usan XML y HTTP. Independientes de SO y lenguaje.
SOAP: protocolo para empaquetar mensajes (XML). Estructura: Envelope, Header (autenticación), Body (datos y parámetros). Estilos: RPC (como método) o Document (esquema XML libre, ej AFIP). Codificación: Literal (esquema XML estándar) o Encoded (no estándar, no recomendado).
WSDL: notación para describir el servicio. Elementos: Definitions, Types, Message, PortType, Binding, Service (endpoint).
GraphQL: lenguaje de consulta para APIs. Único endpoint, cliente pide exactamente lo que necesita. Schema tipado. Soporta queries, mutations, subscriptions. Más flexible que REST.

gRPC / Protobuf: Protocol Buffers es formato binario de serialización. Servicios definidos en archivos .proto (IDL). gRPC usa HTTP/2, comunicación bidireccional por streams, seguridad integrada, balanceo de carga. Desventajas: limitaciones con navegadores, curva de aprendizaje, configuración compleja.

OBJETIVOS DE LA ARQUITECTURA DE SOFTWARE:
Exponer estructura del sistema pero ocultar detalles de implementación.
Cumplir con todos los casos de uso y escenarios.
Abordar requisitos de partes interesadas (stakeholders).
Manejar requerimientos funcionales y de calidad.
Ser flexible al cambio.
El software debe construirse sobre fundación sólida considerando escenarios clave.

PRINCIPIOS CLAVE DE ARQUITECTURA (4):
1. Construir para el cambio (no para durar): anticipar evolución, incorporar flexibilidad desde el inicio.
2. Modelar para analizar y reducir riesgos: usar UML, visualizaciones, capturar decisiones arquitectónicas, estudiar impacto. No formalizar en exceso.
3. Usar modelos como herramientas de comunicación y colaboración: transmisión clara del diseño a todos los stakeholders, facilitar comunicación ante modificaciones.
4. Identificar decisiones clave de ingeniería: conocer dónde se cometen errores, invertir tiempo en acertar estas decisiones desde el inicio.
Enfoque incremental e iterativo: arquitectura base → candidatas → probar → mejorar. No sobreingeniería. Evitar BDUF cuando requisitos no están claros.

PRINCIPIOS CLAVE DE DISEÑO (5):
1. Separación de responsabilidades: dividir en funcionalidades distintas con mínima superposición. Alta cohesión interna, bajo acoplamiento. Separar en límites incorrectos puede empeorar el acoplamiento.
2. Principio de responsabilidad única: cada componente/módulo encargado de una única funcionalidad o conjunto estrechamente relacionado.
3. Principio del menor conocimiento (Ley de Demeter): un componente no debe conocer detalles internos de otros.
4. DRY (Don't Repeat Yourself): cada intención expresada en un único lugar. Una funcionalidad en un solo componente.
5. Minimizar diseño inicial (YAGNI / BDUF): diseñar solo lo necesario. YAGNI: "No lo vas a necesitar". BDUF: Big Design Up Front (evitar en contextos ágiles con requisitos inciertos).

PRÁCTICAS DE DISEÑO (6):
1. Mantener consistentes los patrones de diseño dentro de cada capa (no mezclar paradigmas sin razón).
2. No duplicar funcionalidad dentro de la aplicación (cada función en un único componente).
3. Preferir composición antes que herencia (la herencia incrementa acoplamiento y genera jerarquías complejas).
4. Establecer estilo de codificación y convenciones de nombres (uniformidad, facilita revisiones y mantenimiento).
5. Mantener calidad con técnicas automatizadas (pruebas unitarias, análisis de dependencias, análisis estático, métricas de rendimiento).
6. Considerar la operación de la aplicación (datos operativos y métricas para despliegue y ejecución eficiente).

ASPECTOS TRANSVERSALES: Registro e instrumentación, Autenticación, Autorización, Gestión de excepciones, Comunicación, Caching.

MODELOS PEER-TO-PEER (P2P): Todos los procesos actúan como pares, sin jerarquía. Descentralizado. Cada nodo mantiene consistencia de recursos.
CORBA: Precursor de web services. IDL para interfaces contractuales. ORB (Object Request Broker) comunica objetos independientemente de su localización.
SISTEMAS MONOLÍTICOS: Único elemento funcional (interfaz, datos, lógica). Un lenguaje, una plataforma. Primer paso hacia modularización: programación estructurada.
INTEGRACIÓN: Punto a punto (rígido, poco escalable), por adaptadores (hub + adaptadores), mediador de mensajes (hub genera salidas válidas para clientes).
ERP (Enterprise Resource Planning): integración por repositorio único. CRM: apoyo a gestión de clientes/ventas. DataWarehouse: datos orientados a decisiones.

LEY DE DEMETER: Principio de Conocimiento Mínimo. Un objeto no debe conocer internals de otros.
YAGNI: "You ain't gonna need it". BDUF: Big Design Up Front. DRY: Don't Repeat Yourself.
Un estilo consistente mejora la legibilidad y mantenibilidad del código.
Se deben definir métricas claras de comportamiento y rendimiento.
`;

const QUESTION_BANK = [
  { id: 1, type: "desarrollo", topic: "Características SD", question: "Nombra y desarrolla al menos 3 características de los sistemas distribuidos según Colouris.", keywords: ["comparticion recursos", "apertura", "interfaces publicas", "concurrencia", "procesos paralelos", "escalabilidad", "tolerancia fallos", "redundancia", "transparencia"] },
  { id: 2, type: "desarrollo", topic: "Apertura", question: "¿Qué significa que un sistema distribuido sea 'abierto'? Desarrolle el concepto de Apertura.", keywords: ["interfaces", "especificadas", "accesibles", "desarrolladores", "extensibilidad", "interoperabilidad", "estandares", "heterogeneo", "extension hardware", "extension software"] },
  { id: 3, type: "desarrollo", topic: "Concurrencia", question: "Desarrolle el concepto de Concurrencia en sistemas distribuidos.", keywords: ["simultanea", "paralelo", "procesos", "entrelazando", "sincronizacion", "deadlock", "multiples", "servidores concurrentes", "usuarios interactuan"] },
  { id: 4, type: "desarrollo", topic: "MOM", question: "¿Qué es MOM (Message Oriented Middleware)? Desarrolle el concepto.", keywords: ["middleware orientado mensajes", "asincrono", "mensajes encolados", "cola", "destinatario", "kafka", "no requiere disponibilidad simultanea", "desacopla"] },
  { id: 5, type: "choice", topic: "Transparencias", question: "¿Cuáles son las transparencias MÁS CRÍTICAS en un sistema distribuido?", options: ["Transparencia de acceso y localización", "Transparencia de replicación y fallos", "Transparencia de migración y escalado", "Transparencia de concurrencia y prestaciones"], correct: 0 },
  { id: 6, type: "completar", topic: "Middleware", question: "Un ejemplo de middleware específico es ________ para bases de datos.", answer: "ODBC", keywords: ["odbc"] },
  { id: 7, type: "completar", topic: "Apertura", question: "Un sistema es abierto si sus ________ están claramente especificados y accesibles a los desarrolladores.", answer: "interfaces", keywords: ["interfaces"] },
  { id: 8, type: "vf", topic: "SD General", question: "Un sistema distribuido únicamente se implementa sobre redes de área local.", correct: false, explanation: "Falso. Un sistema distribuido puede implementarse sobre cualquier tipo de red, desde LAN hasta Internet o WAN." },
  { id: 9, type: "completar", topic: "Sockets", question: "Al utilizar ________ el programador debe resolver las diferencias en la representación de datos.", answer: "sockets", keywords: ["sockets"] },
  { id: 10, type: "completar", topic: "Diseño", question: "La Ley de ________ también se conoce como Principio de Conocimiento Mínimo.", answer: "Demeter", keywords: ["demeter"] },
  { id: 11, type: "completar", topic: "Diseño", question: "Minimizar el diseño anticipado también se llama principio ________.", answer: "YAGNI", keywords: ["yagni", "bduf"] },
  { id: 12, type: "completar", topic: "Diseño", question: "La herencia incrementa ________ entre clases padre e hija.", answer: "acoplamiento", keywords: ["acoplamiento", "dependencia"] },
  { id: 13, type: "desarrollo", topic: "Transparencia de Acceso", question: "Desarrolle la transparencia de acceso en sistemas distribuidos.", keywords: ["acceso", "remoto", "local", "mismo", "objetos", "recursos", "sin distincion", "forma identica"] },
  { id: 14, type: "desarrollo", topic: "Transparencia de Replicación", question: "Desarrolle la transparencia de replicación en sistemas distribuidos.", keywords: ["replicas", "copias", "usuario no sabe", "multiple servidores", "fiabilidad", "rendimiento", "instancias"] },
  { id: 15, type: "listado", topic: "Principios Arquitectura", question: "Nombra 4 principios clave de arquitectura de software y desarrolla uno.", keywords: ["cambio", "modelar riesgos", "comunicacion visual", "decisiones ingenieria", "uml", "colaboracion", "flexible", "stakeholders"] },
  { id: 16, type: "listado", topic: "Principios Diseño", question: "Nombra 4 principios clave de diseño y desarrolla uno.", keywords: ["separacion responsabilidades", "responsabilidad unica", "menor conocimiento", "demeter", "dry", "yagni", "bduf", "cohesion", "acoplamiento"] },
  { id: 17, type: "listado", topic: "Prácticas Diseño", question: "Nombra 4 prácticas de diseño y desarrolla una.", keywords: ["patrones consistentes", "no duplicar", "composicion sobre herencia", "estilo codificacion", "calidad automatica", "convenciones nombres", "operacion aplicacion"] },
  { id: 18, type: "listado", topic: "Bus de Mensajes", question: "Mencione 5 beneficios de la arquitectura de Bus de Mensajes.", keywords: ["extensibilidad", "baja complejidad", "flexibilidad", "escalabilidad", "bajo acoplamiento", "simplicidad", "configuracion"] },
  { id: 19, type: "desarrollo", topic: "Bus de Mensajes", question: "¿En qué caso utilizarías la arquitectura Bus de Mensajes? Desarrolle.", keywords: ["multiples aplicaciones", "interoperan", "asincrono", "microservicios", "integracion", "sistemas heterogeneos", "eventos", "desacoplamiento"] },
  { id: 20, type: "listado", topic: "Capas", question: "Menciona al menos 4 beneficios de la arquitectura en Capas.", keywords: ["abstraccion", "aislamiento", "manejabilidad", "rendimiento", "testabilidad", "reusabilidad", "independencia"] },
  { id: 21, type: "listado", topic: "SOA", question: "Menciona al menos 4 beneficios de la arquitectura Orientada a Servicios (SOA).", keywords: ["alineacion dominio", "abstraccion", "interoperabilidad", "descubrimiento", "reutilizacion", "bajo acoplamiento", "contratos"] },
  { id: 22, type: "desarrollo", topic: "SOA", question: "¿Dónde se aplicarían los criterios de uso de la arquitectura SOA? Desarrolle.", keywords: ["sistemas empresariales", "plataformas heterogeneas", "erp", "crm", "servicios descubribles", "autenticacion federada", "integracion", "autonomos"] },
  { id: 23, type: "listado", topic: "Cliente-Servidor", question: "Menciona 4 principios clave del modelo Cliente-Servidor.", keywords: ["separacion roles", "autorizacion servidor", "comunicacion protocolos", "peticion respuesta", "cliente inicia"] },
  { id: 24, type: "desarrollo", topic: "RPC", question: "Desarrolle el concepto de RPC en sistemas distribuidos.", keywords: ["llamada procedimiento remoto", "como local", "idl", "marshalling", "stub", "skeleton", "transparencia", "binding", "sincrono"] },
  { id: 25, type: "desarrollo", topic: "Objetivos Arquitectura", question: "Desarrolle los objetivos de la arquitectura de software.", keywords: ["exponer estructura", "ocultar implementacion", "cumplir casos uso", "requisitos calidad", "flexible al cambio", "partes interesadas", "stakeholders"] },
  { id: 26, type: "choice", topic: "Middleware", question: "¿Cuál es la función principal del Middleware en sistemas distribuidos?", options: ["Facilitar la comunicación entre clientes y servidores con acceso transparente", "Gestionar la base de datos central del sistema", "Reemplazar al sistema operativo en sistemas distribuidos", "Controlar el hardware de la red"], correct: 0 },
  { id: 27, type: "listado", topic: "Características SD", question: "Nombra las 6 características de los sistemas distribuidos según Colouris.", keywords: ["comparticion", "apertura", "concurrencia", "escalabilidad", "tolerancia", "transparencia"] },
  { id: 28, type: "desarrollo", topic: "Tolerancia a Fallos", question: "Desarrolle la tolerancia a fallos en sistemas distribuidos.", keywords: ["redundancia hardware", "recuperacion software", "rollback", "replicacion servidores", "alta disponibilidad", "fallo sin detener sistema", "componentes redundantes"] },
  { id: 29, type: "choice", topic: "Diseño", question: "¿Qué principio indica que se debe preferir composición sobre herencia?", options: ["DRY", "YAGNI", "Práctica de diseño: preferir composición antes que herencia", "Principio de responsabilidad única"], correct: 2 },
  { id: 30, type: "vf", topic: "Diseño", question: "La Ley de Demeter también se conoce como el Principio del Mayor Conocimiento.", correct: false, explanation: "Falso. La Ley de Demeter se conoce como el Principio del MENOR Conocimiento (Conocimiento Mínimo)." },
  // Preguntas agregadas desde los exámenes reales
  { id: 31, type: "listado", topic: "DDD", question: "Menciona 3 beneficios del Diseño Impulsado por el Dominio (DDD).", keywords: ["comunicacion efectiva", "lenguaje comun", "extensible", "modular", "comprobable", "testable", "bajo acoplamiento", "cohesivos"] },
  { id: 32, type: "listado", topic: "Bus de Mensajes", question: "Nombra los principios clave de la arquitectura Bus de Mensajes y desarrolla uno.", keywords: ["comunicacion mensajes", "esquemas contratos compartidos", "procesamiento modular", "asincrono", "bus comun"] },
  { id: 33, type: "completar", topic: "Diseño", question: "Un estilo consistente mejora ________ del código.", answer: "legibilidad", keywords: ["legibilidad", "mantenibilidad", "uniformidad", "revision"] },
  { id: 34, type: "completar", topic: "Diseño", question: "Se deben definir métricas claras de ________ y rendimiento.", answer: "comportamiento", keywords: ["comportamiento", "performance", "calidad"] },
  { id: 35, type: "desarrollo", topic: "Escalabilidad", question: "Desarrolle el concepto de Escalabilidad en sistemas distribuidos.", keywords: ["escala diferente", "miles computadoras", "software no cambia", "replicacion datos", "caching", "multiples servidores", "concurrencia", "eficiente"] },
  { id: 36, type: "desarrollo", topic: "Transparencia de Localización", question: "Desarrolle la transparencia de localización en sistemas distribuidos.", keywords: ["acceder", "sin conocer", "ubicacion", "localizacion", "recurso", "donde se encuentra", "anonimato"] },
  { id: 37, type: "desarrollo", topic: "Transparencia de Fallos", question: "Desarrolle la transparencia de fallos en sistemas distribuidos.", keywords: ["continuar", "tareas", "pese a errores", "hardware", "software", "fallo", "usuario no nota", "sistema sigue"] },
  { id: 38, type: "desarrollo", topic: "Middleware", question: "Desarrolle el concepto de Middleware en sistemas distribuidos.", keywords: ["software intermedio", "comunicacion", "clientes servidores", "abstraccion", "heterogeneidad", "transparente", "api", "tcp ip", "independizar entornos"] },
  { id: 39, type: "listado", topic: "Cliente-Servidor", question: "Menciona al menos 4 beneficios del modelo Cliente-Servidor.", keywords: ["seguridad", "acceso centralizado", "mantenimiento", "interoperabilidad", "modularidad", "escalabilidad", "datos en servidor", "plataformas distintas"] },
  { id: 40, type: "desarrollo", topic: "Apertura", question: "¿Qué tres características debe tener un sistema distribuido para considerarse abierto? Desarrolle.", keywords: ["interfaces publicas", "mecanismo uniforme comunicacion", "hardware software heterogeneo", "estandares", "extensibilidad", "interoperabilidad"] },
  { id: 41, type: "vf", topic: "RPC", question: "En RPC, los parámetros se pueden pasar por referencia ya que existe memoria compartida entre cliente y servidor.", correct: false, explanation: "Falso. En RPC no existe memoria compartida, por lo que los parámetros siempre se pasan por valor." },
  { id: 42, type: "vf", topic: "MOM", question: "MOM requiere que el emisor y el receptor estén disponibles simultáneamente para funcionar.", correct: false, explanation: "Falso. Una ventaja clave de MOM es que es asíncrono: los mensajes quedan encolados y el receptor puede leerlos cuando esté disponible." },
  { id: 43, type: "choice", topic: "RPC vs MOM", question: "¿Cuál de las siguientes afirmaciones describe mejor la diferencia entre RPC y MOM?", options: ["RPC es sincrónico (como un teléfono) y MOM es asíncrono (como una oficina postal)", "RPC es más lento y MOM es más rápido", "MOM requiere que ambas partes estén activas al mismo tiempo", "RPC usa colas de mensajes y MOM usa llamadas directas"], correct: 0 },
  { id: 44, type: "listado", topic: "SOA", question: "Nombra 4 principios clave de la arquitectura Orientada a Servicios (SOA) y desarrolla uno.", keywords: ["servicios autonomos", "contratos compartidos", "acoplamiento flexible", "servicios distribuibles", "compatibilidad politicas", "esquemas no clases", "independientes"] },
  { id: 45, type: "desarrollo", topic: "Objetivos SD", question: "Desarrolle los objetivos y desafíos de los sistemas distribuidos.", keywords: ["apertura", "escalabilidad", "accesibilidad", "transparencia", "concurrencia deadlock", "seguridad autenticacion", "heterogeneidad plataformas"] },
  { id: 46, type: "listado", topic: "Orientada a Objetos", question: "Menciona 4 principios de la arquitectura Orientada a Objetos y desarrolla uno.", keywords: ["abstraccion", "encapsulacion", "polimorfismo", "desacoplamiento", "interfaces abstractas", "oculta detalles"] },
  { id: 47, type: "desarrollo", topic: "Sistemas N-Tiered", question: "Desarrolle los sistemas N-Tiered (two-tier y three-tier). Mencione beneficios.", keywords: ["two tier cliente servidor", "three tier capa intermedia", "logica aplicacion", "presentacion", "datos", "modularidad", "escalabilidad", "mantenibilidad"] },
  { id: 48, type: "completar", topic: "RPC", question: "En RPC, el componente del servidor que recibe y resuelve las invocaciones del cliente se llama ________.", answer: "skeleton", keywords: ["skeleton", "stub servidor"] },
  { id: 49, type: "completar", topic: "Arquitectura", question: "El principio que indica que cada componente debe encargarse de una única funcionalidad se llama principio de ________.", answer: "responsabilidad única", keywords: ["responsabilidad unica", "single responsibility"] },
  { id: 50, type: "vf", topic: "Capas", question: "En la arquitectura en capas, la capa de interfaz de usuario puede acceder directamente a la capa de datos sin pasar por la lógica de negocio.", correct: false, explanation: "Falso. En la arquitectura en capas, la comunicación debe seguir el orden establecido entre capas. La UI accede a datos a través de componentes de negocio o de acceso a datos." },
  { id: 51, type: "desarrollo", topic: "Compartición de Recursos", question: "Desarrolle la compartición de recursos en sistemas distribuidos. ¿Qué rol cumplen los gestores de recursos?", keywords: ["hardware disco impresora", "software archivos bd", "encapsulados", "red", "gestor recursos", "nombres", "traduccion direcciones", "accesos concurrentes", "consistencia"] },
  { id: 52, type: "listado", topic: "Basada en Componentes", question: "Menciona los principios de la arquitectura Basada en Componentes y explica uno.", keywords: ["reutilizable", "reemplazable", "encapsulado", "independiente", "extensible", "interfaces bien definidas"] },
  { id: 53, type: "choice", topic: "SD General", question: "Según Tanenbaum, ¿qué componente de software oculta los detalles de ejecución local en un sistema distribuido?", options: ["El sistema operativo distribuido", "El middleware", "El protocolo TCP/IP", "El cliente pesado"], correct: 1 },
  { id: 54, type: "desarrollo", topic: "Comparación MOM vs RPC", question: "Compare MOM y RPC. ¿Cuándo usaría cada uno?", keywords: ["mom asincrono", "rpc sincrono", "mom cola", "rpc llamada directa", "mom desacoplado", "rpc cliente servidor", "eventos mom", "simplicidad rpc", "heterogeneo mom"] },
  { id: 55, type: "listado", topic: "Objetivos Arquitectura", question: "Nombra 4 objetivos de la arquitectura de software.", keywords: ["exponer estructura", "ocultar implementacion", "casos de uso", "stakeholders", "requerimientos calidad", "flexible", "funcional"] },
];

function shuffle(arr) {
  const s = [...arr];
  for (let i = s.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [s[i], s[j]] = [s[j], s[i]];
  }
  return s;
}

const STORAGE_KEY = "sd_exam_used_ids";
const EXAM_SIZE = 7;

async function getUsedIds() {
  try {
    const result = await window.storage.get(STORAGE_KEY);
    return result ? JSON.parse(result.value) : [];
  } catch {
    return [];
  }
}

async function saveUsedIds(ids) {
  try {
    await window.storage.set(STORAGE_KEY, JSON.stringify(ids));
  } catch {}
}

async function generateExam() {
  let usedIds = await getUsedIds();
  const allIds = QUESTION_BANK.map(q => q.id);

  // Si ya se usaron todas o casi todas, resetear el historial
  const remaining = allIds.filter(id => !usedIds.includes(id));
  if (remaining.length < EXAM_SIZE) {
    usedIds = [];
    await saveUsedIds([]);
  }

  const available = QUESTION_BANK.filter(q => !usedIds.includes(q.id));
  const selected = shuffle(available).slice(0, EXAM_SIZE);
  const newUsedIds = [...usedIds, ...selected.map(q => q.id)];
  await saveUsedIds(newUsedIds);
  return selected;
}

function scoreLocal(question, userAnswer) {
  if (!userAnswer || userAnswer.trim() === "") return 0;
  if (question.type === "choice") return parseInt(userAnswer) === question.correct ? 4 : 0;
  if (question.type === "vf") return (userAnswer === "true") === question.correct ? 4 : 0;
  if (question.type === "completar") {
    const ua = userAnswer.toLowerCase().trim();
    return question.keywords.some(k => ua.includes(k.toLowerCase())) ? 4 : 0;
  }
  const ua = userAnswer.toLowerCase();
  let hits = 0;
  for (const kw of question.keywords) {
    if (ua.includes(kw.toLowerCase())) hits++;
  }
  const ratio = hits / question.keywords.length;
  if (ratio >= 0.6) return 4;
  if (ratio >= 0.4) return 3;
  if (ratio >= 0.2) return 2;
  if (hits > 0) return 1;
  return 0;
}

function labelScore(s) {
  if (s === 4) return { label: "Excelente", color: "#00e676", bg: "rgba(0,230,118,0.12)" };
  if (s === 3) return { label: "Bien", color: "#69f0ae", bg: "rgba(105,240,174,0.12)" };
  if (s === 2) return { label: "Regular", color: "#ffd740", bg: "rgba(255,215,64,0.12)" };
  if (s === 1) return { label: "Deficiente", color: "#ff6e40", bg: "rgba(255,110,64,0.12)" };
  return { label: "Incorrecto", color: "#ff1744", bg: "rgba(255,23,68,0.12)" };
}

async function evaluateWithAI(question, userAnswer) {
  const prompt = `Eres un profesor experto en Sistemas Distribuidos evaluando la respuesta de un estudiante universitario.

**Base de Conocimiento (fragmento relevante):**
${KNOWLEDGE_BASE.substring(0, 4000)}

**Pregunta:**
"${question.question}"

**Tipo de pregunta:** ${question.type}

**Respuesta del Estudiante:**
"${(userAnswer || "").substring(0, 1000)}"

**Rúbrica de evaluación:**
- 0: Vacía, irrelevante o completamente incorrecta
- 1: Menciona 1 concepto clave, muy superficial o con errores importantes
- 2: Menciona 2-3 conceptos correctos, explicación incompleta pero va por buen camino
- 3: Menciona la mayoría de conceptos clave (>60%), explicación correcta pero mejorable
- 4: Desarrollo completo, menciona TODOS los conceptos clave, clara, precisa y completa

**Instrucciones:**
- Para preguntas tipo "listado": verificar que mencione la cantidad requerida de ítems con al menos su nombre correcto.
- Para preguntas tipo "desarrollo": verificar profundidad conceptual, no solo mención superficial.
- Ser constructivo en el feedback, mencionar qué está bien y qué falta.
- Devuelve SOLO JSON válido sin texto adicional:
{
  "score": <int 0-4>,
  "feedback": "<string constructivo: qué hizo bien y qué conceptos faltan>",
  "conceptsFound": ["<concepto1>", "<concepto2>"]
}`;

  try {
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 1000,
        system: "Eres un profesor evaluador de Sistemas Distribuidos. Responde SOLO con JSON válido, sin texto adicional ni markdown.",
        messages: [{ role: "user", content: prompt }],
      }),
    });
    if (!response.ok) return null;
    const data = await response.json();
    let text = data.content?.[0]?.text || "";
    text = text.replace(/```json\n?/g, "").replace(/```\n?/g, "").trim();
    const parsed = JSON.parse(text);
    if (typeof parsed.score === "number" && parsed.feedback && Array.isArray(parsed.conceptsFound)) {
      return parsed;
    }
    return null;
  } catch {
    return null;
  }
}

const css = `
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
  .app { min-height: 100vh; background: #0a0a0f; color: #e8e8f0; }

  /* HOME */
  .home { min-height: 100vh; display: flex; align-items: center; justify-content: center; position: relative; overflow: hidden; }
  .home-bg { position: absolute; inset: 0; background: radial-gradient(ellipse 80% 60% at 50% 0%, rgba(99,60,255,0.18) 0%, transparent 70%), radial-gradient(ellipse 60% 40% at 80% 80%, rgba(0,200,120,0.1) 0%, transparent 60%); pointer-events: none; }
  .home-content { position: relative; text-align: center; max-width: 480px; padding: 2rem; }
  .badge-pill { display: inline-block; background: rgba(99,60,255,0.2); border: 1px solid rgba(99,60,255,0.4); color: #a78bfa; padding: 6px 16px; border-radius: 99px; font-size: 12px; font-weight: 600; letter-spacing: 0.08em; margin-bottom: 1.5rem; }
  .home-title { font-size: clamp(2.4rem, 6vw, 3.2rem); font-weight: 700; line-height: 1.1; color: #fff; margin-bottom: 1rem; }
  .home-sub { color: #9090aa; font-size: 1rem; line-height: 1.6; margin-bottom: 2rem; }
  .stats-row { display: flex; align-items: center; justify-content: center; gap: 0; margin-bottom: 2.5rem; background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.08); border-radius: 16px; padding: 1rem 0; }
  .stat { flex: 1; text-align: center; }
  .stat-n { display: block; font-size: 2rem; font-weight: 700; color: #fff; }
  .stat-l { display: block; font-size: 12px; color: #6060788; color: #606078; margin-top: 2px; }
  .stat-div { width: 1px; height: 40px; background: rgba(255,255,255,0.1); }
  .btn-primary { width: 100%; background: linear-gradient(135deg, #6b3cff, #4f2dbb); color: #fff; border: none; border-radius: 12px; padding: 14px 24px; font-size: 1rem; font-weight: 600; cursor: pointer; transition: transform 0.15s, box-shadow 0.15s; box-shadow: 0 4px 24px rgba(99,60,255,0.35); }
  .btn-primary:hover { transform: translateY(-2px); box-shadow: 0 8px 32px rgba(99,60,255,0.5); }
  .btn-primary:active { transform: translateY(0); }
  .hint-text { color: #505068; font-size: 13px; margin-top: 1rem; }

  /* EXAM */
  .exam-view { max-width: 680px; margin: 0 auto; padding: 1.5rem 1rem 6rem; }
  .exam-topbar { margin-bottom: 1.5rem; }
  .progress-track { height: 4px; background: rgba(255,255,255,0.08); border-radius: 99px; overflow: hidden; margin-bottom: 12px; }
  .progress-fill { height: 100%; background: linear-gradient(90deg, #6b3cff, #00c87a); border-radius: 99px; transition: width 0.4s cubic-bezier(.4,0,.2,1); }
  .exam-info { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
  .q-counter { font-size: 13px; font-weight: 600; color: #a0a0c0; }
  .q-topic-tag { background: rgba(99,60,255,0.15); border: 1px solid rgba(99,60,255,0.3); color: #a78bfa; padding: 3px 10px; border-radius: 99px; font-size: 12px; font-weight: 500; }
  .answered-count { margin-left: auto; font-size: 12px; color: #505068; }
  .qcard { background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.08); border-radius: 20px; padding: 1.75rem; margin-bottom: 1.5rem; }
  .type-chip { display: inline-flex; align-items: center; gap: 6px; padding: 4px 12px; border-radius: 99px; font-size: 12px; font-weight: 600; margin-bottom: 1rem; }
  .type-desarrollo { background: rgba(99,60,255,0.15); color: #a78bfa; border: 1px solid rgba(99,60,255,0.25); }
  .type-choice { background: rgba(0,200,150,0.12); color: #34d399; border: 1px solid rgba(0,200,150,0.25); }
  .type-completar { background: rgba(255,200,0,0.12); color: #fbbf24; border: 1px solid rgba(255,200,0,0.25); }
  .type-vf { background: rgba(59,130,246,0.12); color: #60a5fa; border: 1px solid rgba(59,130,246,0.25); }
  .type-listado { background: rgba(236,72,153,0.12); color: #f472b6; border: 1px solid rgba(236,72,153,0.25); }
  .q-text { font-size: 1.05rem; font-weight: 500; color: #dcdcf0; line-height: 1.5; margin-bottom: 1.5rem; }
  .txt-answer { width: 100%; background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 12px; color: #e8e8f0; padding: 12px 16px; font-size: 0.95rem; line-height: 1.6; resize: vertical; outline: none; transition: border-color 0.2s; font-family: inherit; }
  .txt-answer:focus { border-color: rgba(99,60,255,0.5); }
  .inp-answer { width: 100%; background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 12px; color: #e8e8f0; padding: 12px 16px; font-size: 1rem; outline: none; transition: border-color 0.2s; font-family: inherit; }
  .inp-answer:focus { border-color: rgba(255,200,0,0.5); }
  .options { display: flex; flex-direction: column; gap: 10px; }
  .opt { display: flex; align-items: center; gap: 12px; background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.08); border-radius: 12px; padding: 12px 16px; color: #c0c0d8; font-size: 0.95rem; cursor: pointer; text-align: left; transition: all 0.15s; }
  .opt:hover { background: rgba(255,255,255,0.07); border-color: rgba(255,255,255,0.15); }
  .opt-selected { background: rgba(0,200,150,0.1) !important; border-color: rgba(0,200,150,0.4) !important; color: #34d399 !important; }
  .opt-letter { width: 28px; height: 28px; border-radius: 50%; background: rgba(255,255,255,0.08); display: flex; align-items: center; justify-content: center; font-size: 12px; font-weight: 700; flex-shrink: 0; color: #8080a0; }
  .opt-selected .opt-letter { background: rgba(0,200,150,0.2); color: #34d399; }
  .vf-row { display: flex; gap: 12px; }
  .vf-btn { flex: 1; padding: 14px; border-radius: 12px; border: 1px solid rgba(255,255,255,0.1); background: rgba(255,255,255,0.04); color: #c0c0d8; font-size: 1rem; font-weight: 600; cursor: pointer; transition: all 0.15s; }
  .vf-btn:hover { background: rgba(255,255,255,0.08); }
  .vf-v { background: rgba(0,200,150,0.12) !important; border-color: rgba(0,200,150,0.4) !important; color: #34d399 !important; }
  .vf-f { background: rgba(255,80,80,0.12) !important; border-color: rgba(255,80,80,0.4) !important; color: #f87171 !important; }
  .nav-row { display: flex; align-items: center; gap: 12px; }
  .btn-nav { background: rgba(255,255,255,0.06); border: 1px solid rgba(255,255,255,0.1); color: #a0a0c0; padding: 10px 18px; border-radius: 10px; font-size: 0.9rem; cursor: pointer; transition: all 0.15s; }
  .btn-nav:hover:not(:disabled) { background: rgba(255,255,255,0.1); color: #dcdcf0; }
  .btn-nav:disabled { opacity: 0.3; cursor: not-allowed; }
  .btn-fwd { background: rgba(99,60,255,0.15); border-color: rgba(99,60,255,0.3); color: #a78bfa; }
  .btn-finish { margin-left: auto; background: linear-gradient(135deg, #6b3cff, #4f2dbb); border: none; color: #fff; padding: 10px 20px; border-radius: 10px; font-size: 0.9rem; font-weight: 600; cursor: pointer; transition: all 0.15s; }
  .btn-finish:disabled { opacity: 0.6; cursor: not-allowed; }
  .dots { display: flex; gap: 6px; flex: 1; justify-content: center; flex-wrap: wrap; }
  .dot { width: 10px; height: 10px; border-radius: 50%; background: rgba(255,255,255,0.1); border: none; cursor: pointer; transition: all 0.15s; padding: 0; }
  .dot-cur { background: #6b3cff; transform: scale(1.3); }
  .dot-done { background: rgba(0,200,150,0.5); }

  /* LOADING */
  .loading-screen { min-height: 100vh; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 1.5rem; }
  .spinner { width: 48px; height: 48px; border: 3px solid rgba(99,60,255,0.2); border-top-color: #6b3cff; border-radius: 50%; animation: spin 0.8s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .loading-text { color: #6060a0; font-size: 0.95rem; }

  /* RESULTS */
  .results-view { max-width: 680px; margin: 0 auto; padding: 1.5rem 1rem 4rem; }
  .result-hero { border-radius: 20px; padding: 2.5rem; text-align: center; margin-bottom: 2rem; position: relative; overflow: hidden; }
  .hero-pass { background: linear-gradient(135deg, rgba(0,200,100,0.15), rgba(0,100,80,0.2)); border: 1px solid rgba(0,200,100,0.25); }
  .hero-fail { background: linear-gradient(135deg, rgba(255,100,60,0.12), rgba(150,40,20,0.15)); border: 1px solid rgba(255,100,60,0.25); }
  .hero-emoji { font-size: 3rem; margin-bottom: 0.5rem; }
  .hero-note { font-size: 5rem; font-weight: 800; color: #fff; line-height: 1; }
  .hero-label { font-size: 1.3rem; font-weight: 600; margin: 0.25rem 0; }
  .hero-pass .hero-label { color: #34d399; }
  .hero-fail .hero-label { color: #f87171; }
  .hero-pts { color: #8080a0; font-size: 0.95rem; margin-bottom: 1rem; }
  .score-bar-bg { height: 8px; background: rgba(255,255,255,0.08); border-radius: 99px; overflow: hidden; }
  .score-bar-fill { height: 100%; border-radius: 99px; transition: width 1s ease; }
  .corrections { }
  .corr-title { font-size: 1.1rem; font-weight: 600; color: #c0c0d8; margin-bottom: 1.25rem; }
  .corr-card { background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.07); border-radius: 16px; padding: 1.25rem; margin-bottom: 1rem; }
  .corr-head { display: flex; align-items: center; gap: 10px; margin-bottom: 0.75rem; flex-wrap: wrap; }
  .corr-num { font-size: 12px; color: #505068; font-weight: 600; }
  .corr-topic { font-size: 12px; color: #606080; background: rgba(255,255,255,0.05); padding: 2px 8px; border-radius: 6px; }
  .corr-badge { font-size: 12px; font-weight: 600; padding: 3px 10px; border-radius: 99px; margin-left: auto; }
  .corr-q { font-size: 0.9rem; color: #9090b0; margin-bottom: 0.75rem; line-height: 1.4; }
  .corr-ans { font-size: 0.88rem; background: rgba(255,255,255,0.04); border-radius: 8px; padding: 10px 12px; margin-bottom: 0.75rem; }
  .corr-ans strong { color: #8080a0; }
  .answer-text { color: #c0c0d8; margin-top: 4px; line-height: 1.5; }
  .no-ans { color: #f87171; }
  .corr-feedback { background: rgba(99,60,255,0.08); border: 1px solid rgba(99,60,255,0.2); border-radius: 8px; padding: 10px 12px; margin-bottom: 0.75rem; font-size: 0.88rem; }
  .corr-feedback strong { color: #a78bfa; }
  .feedback-text { color: #c0c0d8; margin-top: 4px; line-height: 1.5; }
  .corr-concepts-found { margin-top: 8px; }
  .corr-concepts-found strong { color: #34d399; font-size: 0.85rem; }
  .concepts-list { color: #34d399; font-size: 0.82rem; margin-top: 4px; opacity: 0.8; }
  .corr-ok { font-size: 0.88rem; background: rgba(0,200,100,0.08); border-radius: 8px; padding: 10px 12px; margin-bottom: 0.75rem; }
  .corr-ok strong { color: #34d399; }
  .explanation { color: #8080a0; font-size: 0.85rem; margin-top: 6px; line-height: 1.4; }
  .corr-kw { font-size: 0.85rem; color: #505068; margin-top: 4px; }
  .corr-kw strong { color: #606080; }
  .result-actions { display: flex; gap: 12px; margin-top: 2rem; }
  .btn-secondary { flex: 1; background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); color: #a0a0c0; padding: 12px; border-radius: 12px; font-size: 0.95rem; cursor: pointer; transition: all 0.15s; }
  .btn-secondary:hover { background: rgba(255,255,255,0.09); color: #dcdcf0; }
  .result-actions .btn-primary { flex: 1; padding: 12px; }

  /* Historial */
  .history-hint { font-size: 12px; color: #404058; text-align: center; margin-top: 0.75rem; }
`;

export default function App() {
  const [phase, setPhase] = useState("home");
  const [questions, setQuestions] = useState([]);
  const [answers, setAnswers] = useState({});
  const [current, setCurrent] = useState(0);
  const [scores, setScores] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [usedCount, setUsedCount] = useState(0);
  const totalQuestions = QUESTION_BANK.length;

  useEffect(() => {
    getUsedIds().then(ids => setUsedCount(ids.length));
  }, []);

  const startExam = useCallback(async () => {
    setIsLoading(true);
    const qs = await generateExam();
    const ids = await getUsedIds();
    setUsedCount(ids.length);
    setQuestions(qs);
    setAnswers({});
    setCurrent(0);
    setScores([]);
    setIsLoading(false);
    setPhase("exam");
  }, []);

  const setAnswer = (qid, val) => setAnswers(prev => ({ ...prev, [qid]: val }));

  const finishExam = async () => {
    setIsLoading(true);
    const localScores = questions.map(q => ({
      question: q,
      userAnswer: answers[q.id] || "",
      score: scoreLocal(q, answers[q.id] || ""),
      aiFeedback: null,
      conceptsFound: null,
    }));

    const devQuestions = questions.filter(q => q.type === "desarrollo" || q.type === "listado");
    for (const q of devQuestions) {
      const ua = answers[q.id] || "";
      if (ua.trim().length > 10) {
        const aiResult = await evaluateWithAI(q, ua);
        if (aiResult) {
          const idx = localScores.findIndex(s => s.question.id === q.id);
          if (idx !== -1) {
            localScores[idx].score = aiResult.score;
            localScores[idx].aiFeedback = aiResult.feedback;
            localScores[idx].conceptsFound = aiResult.conceptsFound;
          }
        }
      }
    }
    setScores(localScores);
    setIsLoading(false);
    setPhase("results");
  };

  const totalMax = questions.length * 4;
  const totalGot = scores.reduce((a, s) => a + s.score, 0);
  const finalNote = totalMax > 0 ? ((totalGot / totalMax) * 10).toFixed(1) : "0.0";
  const passed = parseFloat(finalNote) >= 6;

  const q = questions[current];
  const progressPct = Math.round((usedCount / totalQuestions) * 100);

  if (isLoading && phase !== "exam") {
    return (
      <>
        <style>{css}</style>
        <div className="app">
          <div className="loading-screen">
            <div className="spinner" />
            <div className="loading-text">
              {phase === "home" ? "Generando examen nuevo..." : "Corrigiendo con IA..."}
            </div>
          </div>
        </div>
      </>
    );
  }

  return (
    <>
      <style>{css}</style>
      <div className="app">
        {phase === "home" && (
          <div className="home">
            <div className="home-bg" />
            <div className="home-content">
              <div className="badge-pill">📡 SISTEMAS DISTRIBUIDOS</div>
              <h1 className="home-title">Simulador<br />de Examen</h1>
              <p className="home-sub">Exámenes aleatorios sin repetir preguntas ya vistas, corrección detallada con inteligencia artificial.</p>
              <div className="stats-row">
                <div className="stat">
                  <span className="stat-n">7</span>
                  <span className="stat-l">preguntas</span>
                </div>
                <div className="stat-div" />
                <div className="stat">
                  <span className="stat-n">{totalQuestions}</span>
                  <span className="stat-l">en el banco</span>
                </div>
                <div className="stat-div" />
                <div className="stat">
                  <span className="stat-n">{totalQuestions - usedCount}</span>
                  <span className="stat-l">disponibles</span>
                </div>
              </div>
              <button className="btn-primary" onClick={startExam}>⚡ Generar Examen</button>
              <div className="history-hint">
                {usedCount > 0
                  ? `Ya viste ${usedCount} de ${totalQuestions} preguntas (${progressPct}%). Las que salen ahora serán nuevas.`
                  : "Primera vez — todas las preguntas están disponibles."}
              </div>
            </div>
          </div>
        )}

        {phase === "exam" && q && (
          <div className="exam-view">
            <div className="exam-topbar">
              <div className="progress-track">
                <div className="progress-fill" style={{ width: `${((current + 1) / questions.length) * 100}%` }} />
              </div>
              <div className="exam-info">
                <span className="q-counter">{current + 1} / {questions.length}</span>
                <span className="q-topic-tag">{q.topic}</span>
                <span className="answered-count">{Object.keys(answers).length} respondidas</span>
              </div>
            </div>

            <div className="qcard">
              <div className={`type-chip type-${q.type}`}>
                {q.type === "desarrollo" && "📝 Desarrollo"}
                {q.type === "choice" && "🔘 Múltiple"}
                {q.type === "completar" && "✏️ Completar"}
                {q.type === "vf" && "✓✗ V / F"}
                {q.type === "listado" && "📋 Listado"}
              </div>
              <h2 className="q-text">{q.question}</h2>

              {(q.type === "desarrollo" || q.type === "listado") && (
                <textarea className="txt-answer" placeholder="Escribí tu respuesta aquí. Sé lo más detallado posible para obtener una mejor evaluación..." value={answers[q.id] || ""} onChange={e => setAnswer(q.id, e.target.value)} rows={8} />
              )}
              {q.type === "completar" && (
                <input className="inp-answer" type="text" placeholder="Completá el espacio en blanco..." value={answers[q.id] || ""} onChange={e => setAnswer(q.id, e.target.value)} />
              )}
              {q.type === "choice" && (
                <div className="options">
                  {q.options.map((opt, i) => (
                    <button key={i} className={`opt ${answers[q.id] === String(i) ? "opt-selected" : ""}`} onClick={() => setAnswer(q.id, String(i))}>
                      <span className="opt-letter">{String.fromCharCode(65 + i)}</span>{opt}
                    </button>
                  ))}
                </div>
              )}
              {q.type === "vf" && (
                <div className="vf-row">
                  <button className={`vf-btn ${answers[q.id] === "true" ? "vf-v" : ""}`} onClick={() => setAnswer(q.id, "true")}>✓ Verdadero</button>
                  <button className={`vf-btn ${answers[q.id] === "false" ? "vf-f" : ""}`} onClick={() => setAnswer(q.id, "false")}>✗ Falso</button>
                </div>
              )}
            </div>

            <div className="nav-row">
              <button className="btn-nav" onClick={() => setCurrent(c => c - 1)} disabled={current === 0}>← Anterior</button>
              <div className="dots">
                {questions.map((_, i) => (
                  <button key={i} className={`dot ${i === current ? "dot-cur" : ""} ${answers[questions[i]?.id] ? "dot-done" : ""}`} onClick={() => setCurrent(i)} />
                ))}
              </div>
              {current < questions.length - 1
                ? <button className="btn-nav btn-fwd" onClick={() => setCurrent(c => c + 1)}>Siguiente →</button>
                : <button className="btn-finish" onClick={finishExam} disabled={isLoading}>{isLoading ? "⏳ Corrigiendo..." : "📊 Ver Resultado"}</button>
              }
            </div>
          </div>
        )}

        {phase === "results" && (
          <div className="results-view">
            <div className={`result-hero ${passed ? "hero-pass" : "hero-fail"}`}>
              <div className="hero-emoji">{passed ? "🎉" : "📚"}</div>
              <div className="hero-note">{finalNote}</div>
              <div className="hero-label">{passed ? "¡Aprobado!" : "Desaprobado"}</div>
              <div className="hero-pts">{totalGot} de {totalMax} puntos</div>
              <div className="score-bar-bg">
                <div className="score-bar-fill" style={{ width: `${(totalGot / totalMax) * 100}%`, background: passed ? "#00e676" : "#ff6e40" }} />
              </div>
            </div>

            <div className="corrections">
              <h3 className="corr-title">📋 Correcciones Detalladas</h3>
              {scores.map((s, i) => {
                const info = labelScore(s.score);
                const isDevQ = s.question.type === "desarrollo" || s.question.type === "listado";
                const hasAI = isDevQ && s.aiFeedback;
                return (
                  <div key={i} className="corr-card" style={{ borderLeft: `3px solid ${info.color}` }}>
                    <div className="corr-head">
                      <span className="corr-num">Pregunta {i + 1}</span>
                      <span className="corr-topic">{s.question.topic}</span>
                      <span className="corr-badge" style={{ color: info.color, background: info.bg }}>{info.label} · {s.score}/4</span>
                    </div>
                    <p className="corr-q">{s.question.question}</p>
                    {s.userAnswer
                      ? <div className="corr-ans"><strong>Tu respuesta:</strong><div className="answer-text">{s.userAnswer.length > 400 ? s.userAnswer.substring(0, 400) + "..." : s.userAnswer}</div></div>
                      : <div className="corr-ans no-ans">⚠️ Sin respuesta</div>
                    }
                    {hasAI && (
                      <div className="corr-feedback">
                        <strong>🤖 Corrección IA:</strong>
                        <div className="feedback-text">{s.aiFeedback}</div>
                        {s.conceptsFound?.length > 0 && (
                          <div className="corr-concepts-found">
                            <strong>✅ Conceptos identificados:</strong>
                            <div className="concepts-list">{s.conceptsFound.join(" · ")}</div>
                          </div>
                        )}
                      </div>
                    )}
                    {!hasAI && s.question.type === "choice" && (
                      <div className="corr-ok"><strong>✅ Correcta:</strong> {s.question.options[s.question.correct]}</div>
                    )}
                    {!hasAI && s.question.type === "vf" && (
                      <div className="corr-ok">
                        <strong>✅ Correcta:</strong> {s.question.correct ? "Verdadero" : "Falso"}
                        <div className="explanation">💡 {s.question.explanation}</div>
                      </div>
                    )}
                    {!hasAI && s.question.type === "completar" && (
                      <div className="corr-ok"><strong>✅ Esperada:</strong> {s.question.answer}</div>
                    )}
                    {!hasAI && isDevQ && (
                      <div className="corr-kw"><strong>🔑 Palabras clave:</strong> {s.question.keywords.slice(0, 8).join(", ")}</div>
                    )}
                  </div>
                );
              })}
            </div>

            <div className="result-actions">
              <button className="btn-primary" onClick={startExam}>🔄 Nuevo Examen</button>
              <button className="btn-secondary" onClick={() => setPhase("home")}>🏠 Inicio</button>
            </div>
          </div>
        )}
      </div>
    </>
  );
}
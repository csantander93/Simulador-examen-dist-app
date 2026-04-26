import { useState, useCallback } from "react";
import "./App.css";

const KNOWLEDGE_BASE = `
SISTEMAS DISTRIBUIDOS - BASE DE CONOCIMIENTO COMPLETA

DEFINICIÓN: Un sistema distribuido es aquel en el que los componentes de hardware y software se encuentran en una red de computadoras, comunicando y coordinando sus acciones solo mediante mensajes. (Colouris). Tanenbaum: colección de computadoras independientes que se presentan al usuario como un único sistema.

CARACTERÍSTICAS DE SISTEMAS DISTRIBUIDOS (Colouris):
1. COMPARTICIÓN DE RECURSOS: Los recursos (hardware: discos, impresoras; software: archivos, BD) están encapsulados en máquinas y se accede vía red. Gestores de recursos administran el acceso confiable y consistente.
2. APERTURA (Openness): Un sistema es abierto si sus interfaces están claramente especificadas y accesibles a los desarrolladores. Permite extensión en hardware y software sin afectar lo existente. Garantiza extensibilidad, interoperabilidad. Requiere: interfaces públicas, mecanismo uniforme de comunicación, hardware/software heterogéneo.
3. CONCURRENCIA: Ejecución simultánea de múltiples procesos. Con un procesador se entrelaza; con varios se ejecuta en paralelo. Dos razones: usuarios interactuando simultáneamente, y procesos servidores respondiendo peticiones concurrentes.
4. ESCALABILIDAD: Debe funcionar eficientemente sin importar su tamaño, desde dos estaciones hasta miles de computadoras.
5. TOLERANCIA A FALLOS: El sistema sigue funcionando ante errores. Dos enfoques: redundancia hardware (componentes replicados) y recuperación de software (roll-back).
6. TRANSPARENCIA: Ocultar al usuario la separación física de los componentes. 8 tipos: Acceso, Localización, Concurrencia, Replicación, Fallos, Migración, Prestaciones, Escalado. Las más críticas: transparencia de acceso y localización (transparencias de red).

OBJETIVOS: Apertura, Escalabilidad, Accesibilidad, Transparencia.
DESAFÍOS: Concurrencia (deadlocks), Seguridad (autenticación, permisos, auditoría), Heterogeneidad.

MIDDLEWARE: Software que facilita la comunicación entre clientes y servidores. Tipos: genérico (RPC) y específico de dominio (ODBC para bases de datos). Protocolo: TCP/IP.
RPC: Mecanismo sincrónico, invoca procedimientos remotos como locales. Usa IDL. Funciones: marshalling, gestión comunicaciones, enlazado (binding).
MOM: Comunicación asíncrona mediante mensajes encolados. No requiere disponibilidad simultánea. Ejemplos: Kafka.
SOCKETS: Canal nivel transporte (TCP). Bajo nivel, programador maneja representación de datos, errores, asociación.

MODELO CLIENTE-SERVIDOR:
Principios: Separación cliente/servidor. Autorización en servidor. Comunicación mediante protocolos. Petición-respuesta.
Beneficios: Mayor seguridad, acceso centralizado, facilidad de mantenimiento.

ARQUITECTURA BASADA EN COMPONENTES:
Principios: Reusable, Reemplazable, Encapsulado, Independiente, Extensible.
Beneficios: Fácil despliegue, reducción de costos (componentes de terceros), mitigar complejidad técnica.

DISEÑO IMPULSADO POR DOMINIO (DDD):
Principios: Modelo de dominio expresivo, lenguaje ubicuo compartido, modularidad.
Beneficios: Comunicación (lenguaje común), extensible (modular), comprobable (bajo acoplamiento, testable).

ARQUITECTURA EN CAPAS:
Principios: Abstracción, Encapsulación, Alta cohesión, Bajo acoplamiento (comunicación por interfaces).
Beneficios: Abstracción, aislamiento, manejabilidad, rendimiento, capacidad de prueba (mocks).

BUS DE MENSAJES:
Principios: Comunicación basada en mensajes, esquemas y contratos compartidos, procesamiento modular.
Beneficios: Extensibilidad, baja complejidad, flexibilidad (cambios via configuración), bajo acoplamiento, escalabilidad (múltiples instancias), simplicidad.

ARQUITECTURA 3 NIVELES (N-Tier):
Principios: Independencia de niveles, comunicación asíncrona, descomposición funcional clara.
Beneficios: Mantenibilidad, escalabilidad, flexibilidad, disponibilidad.

ORIENTADA A OBJETOS:
Principios: Abstracción, Encapsulación, Polimorfismo, Desacoplamiento.
Beneficios: Comprensible, reutilizable, probable (testable), extensible.

ORIENTADA AL SERVICIO (SOA):
Principios: Servicios autónomos, contratos compartidos (esquemas no clases), acoplamiento flexible, servicios distribuibles, compatibilidad basada en políticas.
Beneficios: Alineación de dominios (reutilización reduce costos), abstracción, interoperabilidad (estándares multiplataforma), capacidad de descubrimiento.

PRINCIPIOS CLAVE DE ARQUITECTURA:
1. Construir para el cambio (no para durar).
2. Modelar para analizar y reducir riesgos (UML, visualizaciones).
3. Usar modelos como herramientas de comunicación y colaboración.
4. Identificar decisiones clave de ingeniería.

PRINCIPIOS CLAVE DE DISEÑO:
1. Separación de responsabilidades (alta cohesión, bajo acoplamiento).
2. Principio de responsabilidad única.
3. Principio del menor conocimiento (Ley de Demeter).
4. DRY (Don't Repeat Yourself).
5. Minimizar diseño inicial (YAGNI/BDUF).

PRÁCTICAS DE DISEÑO:
1. Mantener consistentes los patrones de diseño dentro de cada capa.
2. No duplicar funcionalidad dentro de la aplicación.
3. Preferir composición antes que herencia.
4. Establecer estilo de codificación y convenciones de nombres.
5. Mantener calidad con técnicas automatizadas.
6. Considerar la operación de la aplicación.

LEY DE DEMETER: Principio de Conocimiento Mínimo.
YAGNI: "You ain't gonna need it". BDUF: Big Design Up Front. DRY: Don't Repeat Yourself.
`;

const QUESTION_BANK = [
  { id: 1, type: "desarrollo", topic: "Características SD", question: "Nombra y desarrolla al menos 3 características de los sistemas distribuidos según Colouris.", keywords: ["comparticion recursos", "apertura", "interfaces publicas", "concurrencia", "procesos paralelos", "escalabilidad", "tolerancia fallos", "redundancia", "transparencia"] },
  { id: 2, type: "desarrollo", topic: "Apertura", question: "¿Qué significa que un sistema distribuido sea 'abierto'? Desarrolle el concepto de Apertura.", keywords: ["interfaces", "especificadas", "accesibles", "desarrolladores", "extensibilidad", "interoperabilidad", "estandares", "heterogeneo"] },
  { id: 3, type: "desarrollo", topic: "Concurrencia", question: "Desarrolle el concepto de Concurrencia en sistemas distribuidos.", keywords: ["simultanea", "paralelo", "procesos", "entrelazando", "sincronizacion", "deadlock", "multiples", "servidores concurrentes"] },
  { id: 4, type: "desarrollo", topic: "MOM", question: "¿Qué es MOM (Message Oriented Middleware)? Desarrolle el concepto.", keywords: ["middleware orientado mensajes", "asincrono", "mensajes encolados", "cola", "destinatario", "kafka", "no requiere disponibilidad simultanea"] },
  { id: 5, type: "choice", topic: "Transparencias", question: "¿Cuáles son las transparencias MÁS CRÍTICAS en un sistema distribuido?", options: ["Transparencia de acceso y localización", "Transparencia de replicación y fallos", "Transparencia de migración y escalado", "Transparencia de concurrencia y prestaciones"], correct: 0 },
  { id: 6, type: "completar", topic: "Middleware", question: "Un ejemplo de middleware específico es ________ para bases de datos.", answer: "ODBC", keywords: ["odbc"] },
  { id: 7, type: "completar", topic: "Apertura", question: "Un sistema es abierto si sus ________ están claramente especificados y accesibles a los desarrolladores.", answer: "interfaces", keywords: ["interfaces"] },
  { id: 8, type: "vf", topic: "SD General", question: "Un sistema distribuido únicamente se implementa sobre redes de área local.", correct: false, explanation: "Falso. Un sistema distribuido puede implementarse sobre cualquier tipo de red, no solo LAN, incluyendo WAN, Internet, etc." },
  { id: 9, type: "completar", topic: "Sockets", question: "Al utilizar ________ el programador debe resolver las diferencias en la representación de datos.", answer: "sockets", keywords: ["sockets"] },
  { id: 10, type: "completar", topic: "Diseño", question: "La Ley de ________ también se conoce como Principio de Conocimiento Mínimo.", answer: "Demeter", keywords: ["demeter"] },
  { id: 11, type: "completar", topic: "Diseño", question: "Minimizar el diseño anticipado también se llama principio ________.", answer: "YAGNI", keywords: ["yagni", "bduf"] },
  { id: 12, type: "completar", topic: "Diseño", question: "La herencia incrementa ________ entre clases padre e hija.", answer: "acoplamiento", keywords: ["acoplamiento", "dependencia"] },
  { id: 13, type: "desarrollo", topic: "Transparencia de Acceso", question: "Desarrolle la transparencia de acceso en sistemas distribuidos.", keywords: ["acceso", "remoto", "local", "mismo", "objetos", "recursos", "sin distincion"] },
  { id: 14, type: "desarrollo", topic: "Transparencia de Replicación", question: "Desarrolle la transparencia de replicación en sistemas distribuidos.", keywords: ["replicas", "copias", "usuario no sabe", "multiple servidores", "fiabilidad", "rendimiento", "consistencia"] },
  { id: 15, type: "listado", topic: "Principios Arquitectura", question: "Nombra 4 principios clave de arquitectura de software y desarrolla uno.", keywords: ["cambio", "modelar riesgos", "comunicacion visual", "decisiones ingenieria", "uml", "colaboracion", "flexible"] },
  { id: 16, type: "listado", topic: "Principios Diseño", question: "Nombra 4 principios clave de diseño y desarrolla uno.", keywords: ["separacion responsabilidades", "responsabilidad unica", "menor conocimiento", "demeter", "dry", "yagni", "bduf"] },
  { id: 17, type: "listado", topic: "Prácticas Diseño", question: "Nombra 4 prácticas de diseño y desarrolla una.", keywords: ["patrones consistentes", "no duplicar", "composicion sobre herencia", "estilo codificacion", "calidad automatica", "convenciones nombres"] },
  { id: 18, type: "listado", topic: "Bus de Mensajes", question: "Mencione 5 beneficios de la arquitectura de Bus de Mensajes.", keywords: ["extensibilidad", "baja complejidad", "flexibilidad", "escalabilidad", "bajo acoplamiento", "simplicidad", "configuracion"] },
  { id: 19, type: "desarrollo", topic: "Bus de Mensajes", question: "¿En qué caso utilizarías la arquitectura Bus de Mensajes? Desarrolle.", keywords: ["multiples aplicaciones", "interoperan", "asincrono", "microservicios", "integracion", "sistemas heterogeneos", "eventos"] },
  { id: 20, type: "listado", topic: "Capas", question: "Menciona al menos 4 beneficios de la arquitectura en Capas.", keywords: ["abstraccion", "aislamiento", "manejabilidad", "rendimiento", "testabilidad", "reusabilidad", "independencia"] },
  { id: 21, type: "listado", topic: "SOA", question: "Menciona al menos 4 beneficios de la arquitectura Orientada a Servicios (SOA).", keywords: ["alineacion dominio", "abstraccion", "interoperabilidad", "descubrimiento", "reutilizacion", "bajo acoplamiento"] },
  { id: 22, type: "desarrollo", topic: "SOA", question: "¿Dónde se aplicarían los criterios de uso de la arquitectura SOA? Desarrolle.", keywords: ["sistemas empresariales", "plataformas heterogeneas", "erp", "crm", "servicios descubribles", "autenticacion federada", "integracion"] },
  { id: 23, type: "listado", topic: "Cliente-Servidor", question: "Menciona 4 principios clave del modelo Cliente-Servidor.", keywords: ["separacion roles", "autorizacion servidor", "comunicacion protocolos", "peticion respuesta", "cliente inicia"] },
  { id: 24, type: "desarrollo", topic: "RPC", question: "Desarrolle el concepto de RPC en sistemas distribuidos.", keywords: ["llamada procedimiento remoto", "como local", "idl", "marshalling", "stub", "skeleton", "transparencia", "binding"] },
  { id: 25, type: "desarrollo", topic: "Objetivos Arquitectura", question: "Desarrolle los objetivos de la arquitectura de software.", keywords: ["exponer estructura", "ocultar implementacion", "cumplir casos uso", "requisitos calidad", "flexible al cambio", "partes interesadas"] },
  { id: 26, type: "choice", topic: "Middleware", question: "¿Cuál es la función principal del Middleware en sistemas distribuidos?", options: ["Facilitar la comunicación entre clientes y servidores con acceso transparente", "Gestionar la base de datos central del sistema", "Reemplazar al sistema operativo en sistemas distribuidos", "Controlar el hardware de la red"], correct: 0 },
  { id: 27, type: "listado", topic: "Características SD", question: "Nombra las 6 características de los sistemas distribuidos según Colouris.", keywords: ["comparticion", "apertura", "concurrencia", "escalabilidad", "tolerancia", "transparencia"] },
  { id: 28, type: "desarrollo", topic: "Tolerancia a Fallos", question: "Desarrolle la tolerancia a fallos en sistemas distribuidos.", keywords: ["redundancia hardware", "recuperacion software", "rollback", "replicacion servidores", "alta disponibilidad", "fallo sin detener sistema"] },
  { id: 29, type: "choice", topic: "Diseño", question: "¿Qué principio indica que se debe preferir composición sobre herencia?", options: ["DRY", "YAGNI", "Práctica de diseño: preferir composición antes que herencia", "Principio de responsabilidad única"], correct: 2 },
  { id: 30, type: "vf", topic: "Diseño", question: "La Ley de Demeter también se conoce como el Principio del Mayor Conocimiento.", correct: false, explanation: "Falso. La Ley de Demeter se conoce como el Principio del MENOR Conocimiento (Conocimiento Mínimo)." }
];

function shuffle(arr) { 
  const shuffled = [...arr];
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
  }
  return shuffled;
}

function generateExam() {
  const shuffled = shuffle(QUESTION_BANK);
  return shuffled.slice(0, 7);
}

function scoreLocal(question, userAnswer) {
  if (!userAnswer || userAnswer.trim() === "") return 0;
  
  if (question.type === "choice") {
    return parseInt(userAnswer) === question.correct ? 4 : 0;
  }
  if (question.type === "vf") {
    return (userAnswer === "true") === question.correct ? 4 : 0;
  }
  if (question.type === "completar") {
    const ua = userAnswer.toLowerCase().trim();
    return question.keywords.some(k => ua.includes(k.toLowerCase())) ? 4 : 0;
  }
  
  // Para desarrollo y listado: corrección por keywords
  const ua = userAnswer.toLowerCase();
  let hits = 0;
  for (const kw of question.keywords) {
    if (ua.includes(kw.toLowerCase())) {
      hits++;
    }
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

async function evaluateWithAI(question, userAnswer, knowledgeBase) {
  try {
    const response = await fetch("/api/evaluate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question, userAnswer, knowledgeBase }),
    });

    if (!response.ok) {
      console.warn(`API error: ${response.status}`);
      return null;
    }

    const parsed = await response.json();
    if (typeof parsed.score === "number" && parsed.feedback && Array.isArray(parsed.conceptsFound)) {
      return parsed;
    }
    return null;
  } catch (error) {
    console.error("Error en evaluación IA:", error);
    return null;
  }
}

export default function App() {
  const [phase, setPhase] = useState("home");
  const [questions, setQuestions] = useState([]);
  const [answers, setAnswers] = useState({});
  const [current, setCurrent] = useState(0);
  const [scores, setScores] = useState([]);
  const [isLoading, setIsLoading] = useState(false);

  const startExam = useCallback(() => {
    setQuestions(generateExam());
    setAnswers({});
    setCurrent(0);
    setScores([]);
    setPhase("exam");
  }, []);

  const setAnswer = (qid, val) => setAnswers(prev => ({ ...prev, [qid]: val }));

  const finishExam = async () => {
    setIsLoading(true);
    
    // Primero, evaluación local rápida
    const localScores = questions.map(q => ({
      question: q,
      userAnswer: answers[q.id] || "",
      score: scoreLocal(q, answers[q.id] || ""),
      aiFeedback: null,
      conceptsFound: null
    }));

    // Evaluación con IA para preguntas de desarrollo y listado
    const devQuestions = questions.filter(q => q.type === "desarrollo" || q.type === "listado");
    
    for (const q of devQuestions) {
      const userAnswer = answers[q.id] || "";
      if (userAnswer.trim().length > 10) { // Solo evaluar respuestas sustanciales
        const aiResult = await evaluateWithAI(q, userAnswer, KNOWLEDGE_BASE);
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

  return (
    <div className="app">
      {phase === "home" && (
        <div className="home">
          <div className="home-bg" />
          <div className="home-content">
            <div className="badge-pill">📡 SISTEMAS DISTRIBUIDOS</div>
            <h1 className="home-title">Simulador<br />de Examen</h1>
            <p className="home-sub">Generá exámenes aleatorios basados en tu material de estudio, respondé y obtené tu nota al instante con correcciones detalladas e inteligencia artificial.</p>
            <div className="stats-row">
              <div className="stat"><span className="stat-n">7</span><span className="stat-l">preguntas</span></div>
              <div className="stat-div" />
              <div className="stat"><span className="stat-n">6</span><span className="stat-l">para aprobar</span></div>
              <div className="stat-div" />
              <div className="stat"><span className="stat-n">∞</span><span className="stat-l">intentos</span></div>
            </div>
            <button className="btn-primary" onClick={startExam}>⚡ Generar Examen</button>
            <p className="hint-text">Desarrollo · Opción múltiple · Completar · V/F · Listado</p>
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
              <textarea 
                className="txt-answer" 
                placeholder="Escribí tu respuesta aquí. Sé lo más detallado posible para obtener una mejor evaluación..." 
                value={answers[q.id] || ""} 
                onChange={e => setAnswer(q.id, e.target.value)} 
                rows={8} 
              />
            )}
            {q.type === "completar" && (
              <input 
                className="inp-answer" 
                type="text" 
                placeholder="Completá el espacio en blanco..." 
                value={answers[q.id] || ""} 
                onChange={e => setAnswer(q.id, e.target.value)} 
              />
            )}
            {q.type === "choice" && (
              <div className="options">
                {q.options.map((opt, i) => (
                  <button 
                    key={i} 
                    className={`opt ${answers[q.id] === String(i) ? "opt-selected" : ""}`} 
                    onClick={() => setAnswer(q.id, String(i))}
                  >
                    <span className="opt-letter">{String.fromCharCode(65 + i)}</span>{opt}
                  </button>
                ))}
              </div>
            )}
            {q.type === "vf" && (
              <div className="vf-row">
                <button 
                  className={`vf-btn ${answers[q.id] === "true" ? "vf-v" : ""}`} 
                  onClick={() => setAnswer(q.id, "true")}
                >
                  ✓ Verdadero
                </button>
                <button 
                  className={`vf-btn ${answers[q.id] === "false" ? "vf-f" : ""}`} 
                  onClick={() => setAnswer(q.id, "false")}
                >
                  ✗ Falso
                </button>
              </div>
            )}
          </div>

          <div className="nav-row">
            <button 
              className="btn-nav" 
              onClick={() => setCurrent(c => c - 1)} 
              disabled={current === 0}
            >
              ← Anterior
            </button>
            <div className="dots">
              {questions.map((_, i) => (
                <button 
                  key={i} 
                  className={`dot ${i === current ? "dot-cur" : ""} ${answers[questions[i]?.id] ? "dot-done" : ""}`} 
                  onClick={() => setCurrent(i)} 
                />
              ))}
            </div>
            {current < questions.length - 1
              ? <button className="btn-nav btn-fwd" onClick={() => setCurrent(c => c + 1)}>Siguiente →</button>
              : <button className="btn-finish" onClick={finishExam} disabled={isLoading}>
                  {isLoading ? "⏳ Corrigiendo con IA..." : "📊 Ver Resultado"}
                </button>
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
              <div 
                className="score-bar-fill" 
                style={{ width: `${(totalGot / totalMax) * 100}%`, background: passed ? "#00e676" : "#ff6e40" }} 
              />
            </div>
          </div>

          <div className="corrections">
            <h3 className="corr-title">📋 Correcciones Detalladas</h3>
            {scores.map((s, i) => {
              const info = labelScore(s.score);
              const isDevQuestion = (s.question.type === "desarrollo" || s.question.type === "listado");
              const hasAIFeedback = isDevQuestion && s.aiFeedback;

              return (
                <div key={i} className="corr-card" style={{ borderLeft: `4px solid ${info.color}` }}>
                  <div className="corr-head">
                    <span className="corr-num">Pregunta {i + 1}</span>
                    <span className="corr-topic">{s.question.topic}</span>
                    <span className="corr-badge" style={{ color: info.color, background: info.bg }}>
                      {info.label} · {s.score}/4
                    </span>
                  </div>
                  <p className="corr-q">{s.question.question}</p>
                  
                  {s.userAnswer ? (
                    <div className="corr-ans">
                      <strong>📝 Tu respuesta:</strong> 
                      <div className="answer-text">{s.userAnswer.length > 400 ? s.userAnswer.substring(0, 400) + "..." : s.userAnswer}</div>
                    </div>
                  ) : (
                    <div className="corr-ans no-ans">⚠️ Sin respuesta</div>
                  )}

                  {/* Feedback de IA para preguntas de desarrollo/listado */}
                  {hasAIFeedback && (
                    <div className="corr-feedback">
                      <strong>🤖 Corrección IA:</strong>
                      <div className="feedback-text">{s.aiFeedback}</div>
                      {s.conceptsFound && s.conceptsFound.length > 0 && (
                        <div className="corr-concepts-found">
                          <strong>✅ Conceptos identificados:</strong>
                          <div className="concepts-list">{s.conceptsFound.join(' • ')}</div>
                        </div>
                      )}
                    </div>
                  )}

                  {/* Feedback tradicional para otros tipos */}
                  {!hasAIFeedback && s.question.type === "choice" && (
                    <div className="corr-ok">
                      <strong>✅ Respuesta correcta:</strong> {s.question.options[s.question.correct]}
                    </div>
                  )}
                  {!hasAIFeedback && s.question.type === "vf" && (
                    <div className="corr-ok">
                      <strong>✅ Respuesta correcta:</strong> {s.question.correct ? "Verdadero" : "Falso"}
                      <div className="explanation">💡 {s.question.explanation}</div>
                    </div>
                  )}
                  {!hasAIFeedback && s.question.type === "completar" && (
                    <div className="corr-ok">
                      <strong>✅ Respuesta esperada:</strong> {s.question.answer}
                    </div>
                  )}
                  {!hasAIFeedback && isDevQuestion && (
                    <div className="corr-kw">
                      <strong>🔑 Palabras clave esperadas:</strong> {s.question.keywords.slice(0, 8).join(", ")}
                    </div>
                  )}
                </div>
              );
            })}
          </div>

          <div className="result-actions">
            <button className="btn-primary" onClick={startExam}>🔄 Nuevo Examen</button>
            <button className="btn-secondary" onClick={() => setPhase("home")}>🏠 Volver al Inicio</button>
          </div>
        </div>
      )}
    </div>
  );
}
export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const { question, userAnswer, knowledgeBase } = req.body;

  if (!question || userAnswer === undefined || !knowledgeBase) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  const prompt = `Eres un profesor experto en Sistemas Distribuidos. Tu tarea es evaluar la respuesta de un estudiante.

**Base de Conocimiento:**
${knowledgeBase.substring(0, 3000)}

**Pregunta:**
"${question.question}"

**Respuesta del Estudiante:**
"${(userAnswer || "").substring(0, 800)}"

**Instrucciones:**
1. Analiza la respuesta según la base de conocimiento.
2. Puntúa del 0 al 4 usando esta rúbrica:
   - 0: Vacía, irrelevante o completamente incorrecta
   - 1: Menciona 1 concepto clave, muy superficial o con errores
   - 2: Menciona 2-3 conceptos, explicación incompleta
   - 3: Menciona mayoría de conceptos (>3), explicación correcta pero mejorable
   - 4: Menciona TODOS los conceptos clave, clara, precisa y completa
3. Devuelve SOLO JSON válido. Estructura EXACTA:
   {
     "score": <int 0-4>,
     "feedback": "<string: Explicación constructiva. Menciona qué conceptos están presentes y cuáles faltan>",
     "conceptsFound": ["<concepto1>", "<concepto2>"]
   }`;

  try {
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": process.env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-6",
        max_tokens: 800,
        system: "Eres un profesor evaluador. Responde SOLO con JSON válido, sin texto adicional ni markdown.",
        messages: [{ role: "user", content: prompt }],
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error("Anthropic API error:", response.status, errorText);
      return res.status(502).json({ error: "AI service error" });
    }

    const data = await response.json();
    let text = data.content?.[0]?.text || "";
    text = text.replace(/```json\n?/g, "").replace(/```\n?/g, "").trim();

    const parsed = JSON.parse(text);
    if (
      typeof parsed.score === "number" &&
      parsed.feedback &&
      Array.isArray(parsed.conceptsFound)
    ) {
      return res.status(200).json(parsed);
    }

    return res.status(422).json({ error: "Invalid AI response format" });
  } catch (error) {
    console.error("Evaluation error:", error);
    return res.status(500).json({ error: "Internal server error" });
  }
}

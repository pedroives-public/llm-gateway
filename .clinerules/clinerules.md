<!-- GERADO por harness/apply.sh. NAO edite aqui: edite no repo harness/ e rode apply.sh. -->

# Harness Core — invariantes (valem em Estudo E Produção)

Carregado todo turno. Vale para qualquer tarefa e qualquer ferramenta.

> GERADO A PARTIR DAQUI. Quando projetado num projeto (`.clinerules/clinerules.md`,
> `CLAUDE.md`, `AGENTS.md`), aquelas cópias são derivadas. NÃO edite as cópias —
> edite este arquivo no repo `harness/` e rode `apply.sh` de novo.

## 0. Pre-flight (uma vez por sessão)
- `which pnpm node` — se faltar, PARE e reporte.
- `pnpm typecheck > /tmp/tc_base.txt 2>&1; echo "exit $?"` — se exit!=0 com
  output vazio, o comando não rodou. PARE. Nunca leia "sem output" como
  "sem erros".

## 1. TypeScript
- Zero erro novo em arquivo que você tocou. `pnpm typecheck` é a verdade.
- Proibido silenciar: `as any`, `@ts-ignore`, `@ts-expect-error`, `!`
  non-null novo, `any` novo. Se precisou suprimir, não resolveu.
- ESM/NodeNext: imports relativos com extensão `.js` no código TS.

## 2. Sem placeholder
- Proibido em arquivo entregue: `TODO:` sem link, `throw new Error('not
  implemented')`, stub vazio, "implementar depois". Não sabe? Não entregue:
  pare e reporte.

## 3. Integridade pós-edição (anti-truncamento)
- Após cada escrita: releia as últimas ~20 linhas. Último caractere coerente
  com o formato (`}`, `;`, newline). Para JSON do harness, valide com
  `python3 -c "import json,sys; json.load(open(sys.argv[1]))" <arquivo>`.
- Truncou 2x no mesmo arquivo: PARE e reporte.

## 4. Honestidade (regra dura)
- Nunca alegue "typecheck/teste passou" sem ter rodado. Cite: comando exato,
  exit code, últimas 3 linhas do output. "exit 0" sem o comando colado não
  é evidência — é proibido.
- Nunca invente API de lib. Antes de chamar método que você não tem certeza
  que existe na versão do `package.json`: confirme (REPL/doc da versão).

## 5. Escopo
- Toque só nos arquivos da tarefa atual. Precisa de mais? PARE, reporte,
  aguarde decisão do Pedro.
- Leia ranges exatos quando indicados. Não leia o arquivo inteiro "por
  garantia" — custa contexto e te deixa raso.

## 6. Decisões fechadas do projeto (não reabrir sem fato novo)
- Auth de API key: HMAC-SHA-256 com pepper, `crypto` nativo do Node. NÃO
  Argon2 (decisão D025: mismatch de entropia).
- Formato de chave: `lkey_<random_32bytes_base64url>`. tenant_id não entra
  no plaintext.
- Stack fechada (Fastify v5, Drizzle/PG, ioredis, BullMQ, Pino, Vitest).
  Não sugerir alternativas.
- Migrations: append-only. Nunca edite migration já aplicada/compartilhada;
  crie a próxima.

## 7. Idioma
- Respostas, comentários e UI em português BR. Termos técnicos podem ficar
  em inglês quando for o nome canônico.

## 8. Abertura de sessão (invariante de raiz — HARNESS.md §7 princípio 5)
- Abra a ferramenta (Claude Code/Cline/OpenCode) DE DENTRO da raiz do repo
  do projeto, nunca do diretório-mãe. Um repo = uma raiz = uma sessão.

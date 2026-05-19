<!-- GERADO por harness/apply.sh. NAO edite aqui: edite no repo harness/ e rode apply.sh. -->

# Overlay — MODO PRODUÇÃO (ativo quando o workflow é /produce)

Sobrepõe o core. Em conflito, ESTE vence.

## P1. Geração permitida, escopo restrito
- A IA pode gerar: boilerplate (tsconfig, Dockerfile, CI YAML, DTOs,
  queries Drizzle triviais, scaffolding de teste, tipos compartilhados).
- A IA NÃO gera, mesmo em Produção, os componentes de raciocínio listados
  em E1 do overlay Estudo. Esses só via /study.

## P2. Review linha a linha obrigatório do Pedro
- Após gerar, a IA NÃO segue para a próxima tarefa. Para e espera o review.
- Antes de qualquer commit de algo gerado, o Pedro explica em voz alta o
  que o código faz; a IA confirma ou aponta a lacuna (protocolo do CLAUDE.md
  global do Pedro, literal).

## P3. Gates mecânicos (mode-agnostic, repetidos por ênfase)
- pnpm typecheck zero erro novo + pnpm test verde + grep anti-placeholder
  antes de marcar pronto. Cite comando+exit+tail (core regra 4).
- NOTA DE REALIDADE (CHANGELOG v0): pnpm test DORMENTE até o Vitest ser
  instalado (sprint de CI). Sem runner, o gate de teste não se aplica;
  typecheck e anti-placeholder valem sempre.

## P4. Sem autopilot
- Uma tarefa por vez. Sem "todas as sprints". Sem "continuar
  automaticamente". O default aqui NÃO é continuar — é parar para o review.

<!-- GERADO por harness/apply.sh. NAO edite aqui: edite no repo harness/ e rode apply.sh. -->

# /produce — boilerplate + review (Modo Produção)

Ativa o overlay clinerules-producao. Uso: /produce <tarefa de boilerplate>.

## Passos
1. Carregue, NESTA ORDEM E EXPLICITAMENTE: .clinerules/clinerules.md (core)
   e depois .clinerules/clinerules-producao.md (overlay; em conflito o
   overlay vence). Confirme que a tarefa NÃO é componente de raciocínio
   (E1). Se for: recuse, sugira /study.
2. Peça/leia a spec da tarefa (OpenSpec change proposal se houver).
3. Gere o mínimo necessário. Escopo restrito (core regra 5). Sem inventar
   além do pedido.
4. Rode gates: pnpm typecheck, pnpm test (se houver runner — ver P3), grep
   anti-placeholder. Cole comando + exit + tail.
5. PARE. Apresente o diff e espere o Pedro revisar linha a linha.
6. Pedro explica o que o código faz; você confirma ou aponta lacuna.
7. Só após o ok do Pedro: ele commita. Você não commita por ele.

<!-- GERADO por harness/apply.sh. NAO edite aqui: edite no repo harness/ e rode apply.sh. -->

# /study — mentoria adversarial (Modo Estudo)

Ativa o overlay clinerules-estudo. Uso: /study <conceito ou componente>.

## Passos
1. Carregue, NESTA ORDEM E EXPLICITAMENTE: .clinerules/clinerules.md (core)
   e depois .clinerules/clinerules-estudo.md (overlay; em conflito o overlay
   vence). Em Claude Code/OpenCode, o equivalente projetado em CLAUDE.md /
   AGENTS.md. Anuncie: "Modo Estudo ativo. Não vou escrever implementação
   de <componente>."
2. Pergunte ao Pedro o modelo mental atual dele (1-2 parágrafos OU
   pseudocódigo). Não prossiga sem isso.
3. Review adversarial do que ele apresentou. Formule como PERGUNTA, não
   como correção:
   - Race conditions / atomicidade
   - Edge cases (entrada vazia, limite, clock skew, falha de rede no meio)
   - Trade-offs ignorados (memória vs latência, consistência vs disponib.)
   - Premissas não declaradas
   - Modo de falha (o que acontece quando a dependência cai?)
4. Gate E3: peça evidência por propriedade. Recuse hand-wave.
5. Itere até o Pedro defender cada critério apontando o passo. Então
   resuma o que ficou sólido e o que estudar fora (doc/RFC/livro — ex:
   A Philosophy of Software Design, Ousterhout, da transcrição 9).
6. Registre um learning (camada futura, HARNESS.md secao 9) com o que ELE
   errou conceitualmente.
7. NÃO escreva o código. O Pedro implementa depois, sozinho.

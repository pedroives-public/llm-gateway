<!-- >>> harness >>> -->
<!-- GERADO por harness/apply.sh. NAO edite aqui: edite no repo harness/ e rode apply.sh. -->
## Harness ativo (gerado)
Invariantes sempre-on: `.clinerules/clinerules.md`.
Modos chaveaveis por tarefa:
- `/study`   -> core + `.clinerules/clinerules-estudo.md` (mentoria; IA nao gera codigo de raciocinio).
- `/produce` -> core + `.clinerules/clinerules-producao.md` (boilerplate + review linha a linha).
Workflows: `.clinerules/workflows/{study,produce}.md`. Fonte unica: repo harness/.
<!-- <<< harness <<< -->

## Skill routing

When the user's request matches an available skill, invoke it via the Skill tool. When in doubt, invoke the skill.

Key routing rules:
- Product ideas/brainstorming → invoke /office-hours
- Strategy/scope → invoke /plan-ceo-review
- Architecture → invoke /plan-eng-review
- Design system/plan review → invoke /design-consultation or /plan-design-review
- Full review pipeline → invoke /autoplan
- Bugs/errors → invoke /investigate
- QA/testing site behavior → invoke /qa or /qa-only
- Code review/diff check → invoke /review
- Visual polish → invoke /design-review
- Ship/deploy/PR → invoke /ship or /land-and-deploy
- Save progress → invoke /context-save
- Resume context → invoke /context-restore

# Reference Design

Pasta de referencia interna para a substituicao progressiva do frontend.

## Regras

- nao executar esta pasta como app paralelo.
- nao copiar HTML bruto para `frontend/` em big bang.
- usar os HTMLs e imagens apenas como referencia visual e estrutural.
- manter `frontend/` como unico app executavel.
- usar `docs/FRONTEND_REFERENCE_INVENTORY.md` como indice operacional oficial.

## Estrutura

- `stitch_home_football_analytics/*/code.html`: prototipos por tela/modulo.
- `stitch_home_football_analytics/*/screen.png`: snapshot da tela correspondente.
- `stitch_home_football_analytics/emerald_pitch/DESIGN.md`: design system de referencia.
- `frontend_delivery_plan.md_refined.html`: referencia narrativa importada.
- `relat_rio_de_auditoria_final_football_analytics.html`: auditoria importada.

## Uso correto

- extrair composicao, hierarquia visual e padroes de layout.
- transplantar apenas o necessario para dentro de `frontend/`.
- validar cada tela contra rota canonica, contrato BFF e coverage-state antes de substituir a tela legada.

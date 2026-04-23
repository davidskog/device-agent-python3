# Git/GitHub Workflow (Egen Fork, Ingen PR)

Detta repo drivs som en egen fork.  
Målet är att vara "tyst" mot upstream tills annat beslutas.

## Principer

- Ingen PR-workflow tills vidare.
- Push sker endast till `origin` (egen fork).
- `upstream` används bara för att hämta ändringar.
- Feature-branch hålls uppdaterad via `rebase` mot egen `main`.

## Remote-modell

- `origin` = `https://github.com/davidskog/device-agent-python3.git`
- `upstream` = `https://github.com/Davra/device-agent-python3.git`

## Dagligt arbetssätt

1. Arbeta i feature-branch (ex: `feature/delivery-robustness`).
2. Commit lokalt:
   - `git add <filer>`
   - `git commit -m "<meddelande>"`
3. Push till egen fork:
   - `git push origin feature/delivery-robustness`

## Synka med upstream utan PR

1. `git fetch upstream`
2. `git checkout main`
3. `git merge upstream/main`
4. `git push origin main`
5. `git checkout feature/delivery-robustness`
6. `git rebase main`
7. `git push --force-with-lease origin feature/delivery-robustness`

## Varför `rebase` på steg 6

- Renare historik på feature-branch.
- Mindre merge-brus.
- Passar bra när branchen inte delas av flera utvecklare.

## Säkerhetsregler

- Använd aldrig `git push --force`; använd endast `--force-with-lease`.
- Push aldrig till `upstream`.
- Om PR blir aktuellt i framtiden: beslutas separat då.

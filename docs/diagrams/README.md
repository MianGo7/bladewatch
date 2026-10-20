# Diagrams

The diagrams of the case study, kept as PlantUML sources so that they are
versioned together with the code and the infrastructure they describe and
every change to a model is visible in the history. Each diagram is listed with
the backlog item that produces it and its status. The conceptual diagrams are
drawn before the prototype is implemented and revised afterwards; the
structure and deployment diagrams are drawn from the implemented code and
from `compose.yaml`.

| File | Diagram | Item | Status |
| --- | --- | --- | --- |
| `architecture-overview.puml` | Component diagram of the target architecture with its layers, the chosen tools, and the existing data warehouse | B1 | done |
| `reading-to-alert-sequence.puml` | Sequence of one reading from the sensor through the broker and the processor to the database, the alert, and the dashboard | B1 | done |
| `alert-states.puml` | State diagram of the alert of one machine | B4 | done |
| `package-components.puml` | Component diagram of the Python package, with the imports between modules and libraries | B9 | open |
| `deployment.puml` | Deployment diagram of the containers, their ports, volumes, and topics | B9 | open |

## Conventions

- Every diagram includes the shared style with `!include _style.puml` and
  opens with `@startuml` followed by its file name.
- Notation is UML 2.5.
- Names of modules, classes, topics, tables, and services match the code and
  `compose.yaml` exactly.
- The diagrams are rendered with
  `plantuml -tsvg -o out docs/diagrams/<file>.puml`. The rendered files are
  generated output and are not versioned.
- The structure diagrams are checked by a test: the arrows of the component
  diagram have to equal the imports of the source files.
- The module `notifier` is a leaf of the package. It imports only the standard
  library and `config`, which a test checks, and `cli` is the only module that uses
  it, so that it appears in the component diagram of B9 below `cli` and above
  `config`.
- The module `scenario` sits above the modules that it runs. It imports `producer`,
  `processor`, `storage`, `notifier`, `sensor`, and `config`, and `cli` is the only
  module that uses it, so that it appears in the component diagram of B9 below `cli`
  and above these modules.
- The package `bench` sits next to `scenario`. It imports `config`, `storage`,
  `scenario`, and `measure`, and starts the producer and the processor as processes
  through the command line, so that it does not import them. The package `measure`
  imports only `config`, and `cli` is the only module that uses the two, so that they
  appear in the component diagram of B9 below `cli`, with `bench` above `scenario`,
  `storage`, and `measure`.
- The state diagram of the alert is checked by a test: its arrows have to equal
  the allowed transitions in `src/bladewatch/rules.py`.

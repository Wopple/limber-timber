<!-- REQUIRED SECTIONS -->

# Problem

---

<!-- Describe the problem you are solving including relevant background information

Examples:

There is a typo in the README.

This functionality is not unit tested.

We frequently want to run the migrations in different datasets. In keeping with DRY, it would be
nice to be able to change the project and dataset in one place. Currently, we would need to
update all the operations' values for database and schema in order to change the dataset. This
isn't DRY, and it requires making changes to files that are committed to git for an ephemeral
operation which is bad form.
-->

# Solution

---

<!-- Describe how you are solving the problem

Examples:

Updated instances of `data type` to `datatype`.

I refactored the code to pull the complex logic into testable pure functions, and added unit tests.

This pull request introduces a templating system that parameterizes the operations. Values can be
specified as coming from a templated value. The format is: `{{ variable_name }}`. This means that
the value will be replaced by the template value of the same name. The default value can be
provided in a YAML file, and be overridden by command line arguments.
-->


<!-- OPTIONAL SECTIONS -->

# Testing

---

<!-- Describe how you verified your solution fixes the problem -->

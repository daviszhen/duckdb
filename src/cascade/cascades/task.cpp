#include "duckdb/cascade/cascades/task.hpp"

namespace duckdb {

void CascadesTaskQueue::Push(CascadesTask task) {
	tasks.push_back(task);
}

bool CascadesTaskQueue::Pop(CascadesTask &task) {
	if (tasks.empty()) {
		return false;
	}
	// Highest promise first; among equals, the most recently pushed - which is what makes the
	// "schedule the child before the parent comes back" pattern (OptimizeInputs) work with a
	// plain stack. ORCA's CJobQueue gives each priority class its own FIFO/LIFO and a limit.
	idx_t best = tasks.size() - 1;
	for (idx_t i = tasks.size(); i-- > 0;) {
		if (static_cast<uint8_t>(tasks[i].promise) > static_cast<uint8_t>(tasks[best].promise)) {
			best = i;
		}
	}
	task = tasks[best];
	tasks.erase(tasks.begin() + static_cast<ptrdiff_t>(best));
	return true;
}

} // namespace duckdb

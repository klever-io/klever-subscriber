package subscriber

func mergeTypes(existing, add []EventType) []EventType {
	set := make(map[EventType]bool)
	for _, t := range existing {
		set[t] = true
	}
	for _, t := range add {
		if !set[t] {
			existing = append(existing, t)
			set[t] = true
		}
	}
	return existing
}

func mergeStrings(existing, add []string) []string {
	set := make(map[string]bool)
	for _, s := range existing {
		set[s] = true
	}
	for _, s := range add {
		if !set[s] {
			existing = append(existing, s)
			set[s] = true
		}
	}
	return existing
}

func removeTypes(existing, remove []EventType) []EventType {
	set := make(map[EventType]bool)
	for _, t := range remove {
		set[t] = true
	}
	var result []EventType
	for _, t := range existing {
		if !set[t] {
			result = append(result, t)
		}
	}
	return result
}

func removeStrings(existing, remove []string) []string {
	set := make(map[string]bool)
	for _, s := range remove {
		set[s] = true
	}
	var result []string
	for _, s := range existing {
		if !set[s] {
			result = append(result, s)
		}
	}
	return result
}

func diffTypes(a, b []EventType) []EventType {
	set := make(map[EventType]bool)
	for _, t := range b {
		set[t] = true
	}
	var result []EventType
	for _, t := range a {
		if !set[t] {
			result = append(result, t)
		}
	}
	return result
}

func diffStrings(a, b []string) []string {
	set := make(map[string]bool)
	for _, s := range b {
		set[s] = true
	}
	var result []string
	for _, s := range a {
		if !set[s] {
			result = append(result, s)
		}
	}
	return result
}

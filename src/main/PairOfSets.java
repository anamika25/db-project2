package main;

import java.util.Set;

public class PairOfSets {
	private Set<String> set1;
	private Set<String> set2;

	public PairOfSets(Set<String> s1, Set<String> s2) {
		this.set1 = s1;
		this.set2 = s2;
	}

	public Set<String> getSet1() {
		return set1;
	}

	public void setSet1(Set<String> set1) {
		this.set1 = set1;
	}

	public Set<String> getSet2() {
		return set2;
	}

	public void setSet2(Set<String> set2) {
		this.set2 = set2;
	}

}

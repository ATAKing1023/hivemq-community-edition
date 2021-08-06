package com.hivemq.migration;

import com.hivemq.migration.meta.PersistenceType;

import java.util.Objects;

/**
 * 新旧持久化类型对
 *
 * @author ankang
 * @since 2021/8/6
 */
public class PersistenceTypePair {

    private final PersistenceType previousType;
    private final PersistenceType currentType;

    public PersistenceTypePair(final PersistenceType previousType, final PersistenceType currentType) {
        this.previousType = previousType;
        this.currentType = currentType;
    }

    public PersistenceType getPreviousType() {
        return previousType;
    }

    public PersistenceType getCurrentType() {
        return currentType;
    }

    @Override
    public String toString() {
        return "PersistentTypePair{" + "previousType=" + previousType + ", currentType=" + currentType + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PersistenceTypePair that = (PersistenceTypePair) o;
        return previousType == that.previousType && currentType == that.currentType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(previousType, currentType);
    }
}

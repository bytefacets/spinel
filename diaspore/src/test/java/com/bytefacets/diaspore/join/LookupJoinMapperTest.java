// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.join;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.bytefacets.diaspore.interner.RowInterner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LookupJoinMapperTest {
    private @Mock(lenient = true) JoinListener listener;
    private @Mock(lenient = true) JoinInterner interner;
    private @Mock(lenient = true) RowInterner left;
    private @Mock(lenient = true) RowInterner right;
    private LookupJoinMapper mapper;

    @BeforeEach
    void setUp() {
        when(interner.left()).thenReturn(left);
        when(interner.right()).thenReturn(right);
        setUpInternerWithMapping(1, 5, 10);
    }

    @Nested
    class InnerTests {
        @BeforeEach
        void setUp() {
            mapper = new LookupJoinMapper(interner, listener, 2, 2, false);
        }

        @Nested
        class AddTests {
            @Test
            void leftAddShouldNotEmitJoinWhenUnmapped() {
                mapper.leftRowAdd(1);
                verifyNoInteractions(listener);
            }

            @Test
            void rightAddShouldNotEmitJoinWhenUnmapped() {
                mapper.rightRowAdd(1);
                verifyNoInteractions(listener);
            }

            @Test
            void leftAddShouldEmitJoinWhenMapped() {
                mapper.rightRowAdd(5);
                mapper.leftRowAdd(1);
                verify(listener, times(1)).joinAdded(1);
                assertThat(mapper.leftMapper().sourceRowOf(1), equalTo(1));
                assertThat(mapper.rightMapper().sourceRowOf(1), equalTo(5));
            }

            @Test
            void rightAddShouldEmitJoinUpdateWhenMapped() {
                setUpInternerWithMapping(1, 5, 10);
                mapper.leftRowAdd(1);
                verify(listener, never()).joinAdded(1);
                mapper.rightRowAdd(5);
                verify(listener, times(1)).joinAdded(1);
                assertThat(mapper.leftMapper().sourceRowOf(1), equalTo(1));
                assertThat(mapper.rightMapper().sourceRowOf(1), equalTo(5));
            }
        }

        @Nested
        class ChangeTests {
            @BeforeEach
            void setUp() {
                // mapped pair
                setUpInternerWithMapping(20, 21, 2);
                mapper.leftRowAdd(20);
                mapper.rightRowAdd(21);
                setUpInternerWithMapping(22, 23, 5);
                mapper.leftRowAdd(22);
                mapper.rightRowAdd(23);
                // only left
                setUpInternerWithMapping(30, -1, 3);
                mapper.leftRowAdd(30);
                // only right
                setUpInternerWithMapping(-1, 40, 4);
                mapper.rightRowAdd(40);
                reset(listener);
            }

            @Test
            void leftChangeShouldEmitUpdateWhenMapped() {
                mapper.leftRowChange(20, false);
                verify(listener, times(1)).joinUpdated(20, false, false);
            }

            @Test
            void rightChangeShouldEmitUpdateWhenMapped() {
                mapper.rightRowChange(21, false);
                verify(listener, times(1)).joinUpdated(20, false, false);
            }

            @Test
            void leftChangeShouldNotEmitUpdateWhenUnmapped() {
                mapper.leftRowChange(30, false);
                verifyNoInteractions(listener);
            }

            @Test
            void rightChangeShouldNotEmitUpdateWhenUnmapped() {
                mapper.rightRowChange(40, false);
                verifyNoInteractions(listener);
            }

            @Test
            void rightKeyChangeShouldRemoveOldJoinAndAddNewJoinWhenLeftForBothKeys() {
                setUpInternerWithMapping(20, -1, 2);
                setUpInternerWithMapping(30, 21, 3);
                mapper.rightRowChange(21, true);
                verify(listener, times(1)).joinRemoved(20);
                verify(listener, times(1)).joinAdded(30);
            }

            @Test
            void leftKeyChangeShouldEmitNewJoinWhenOldKeyUnmappedAndNewKeyMapped() {
                setUpInternerWithMapping(30, 40, 4);
                mapper.leftRowChange(30, true);
                verify(listener, times(1)).joinAdded(30);
            }

            @Test
            void leftKeyChangeShouldEmitRemoveJoinWhenOldKeyMappedAndNewKeyUnmapped() {
                setUpInternerWithMapping(20, -1, 10);
                mapper.leftRowChange(20, true);
                verify(listener, times(1)).joinRemoved(20);
            }

            @Test
            void leftKeyChangeShouldEmitJoinUpdateWhenOldKeyMappedAndNewKeyMapped() {
                setUpInternerWithMapping(20, 40, 4);
                mapper.leftRowChange(20, true);
                verify(listener, times(1)).joinUpdated(20, false, true);
            }
        }

        @Nested
        class RemoveTests {
            @BeforeEach
            void setUp() {
                // mapped pair
                setUpInternerWithMapping(20, 21, 2);
                mapper.leftRowAdd(20);
                mapper.rightRowAdd(21);
                // only left
                setUpInternerWithMapping(30, -1, 3);
                mapper.leftRowAdd(30);
                // only right
                setUpInternerWithMapping(-1, 40, 4);
                mapper.rightRowAdd(40);
                reset(listener);
            }

            @Test
            void leftRemoveShouldNotRemoveJoinWhenUnmapped() {
                mapper.leftRowRemove(30);
                verifyNoInteractions(listener);
            }

            @Test
            void rightRemoveShouldNotRemoveJoinWhenUnmapped() {
                mapper.rightRowRemove(40);
                verifyNoInteractions(listener);
            }

            @Test
            void leftRemoveShouldRemoveJoinWhenMapped() {
                mapper.leftRowRemove(20);
                verify(listener, times(1)).joinRemoved(20);
            }

            @Test
            void rightRemoveShouldRemoveJoinWhenMapped() {
                mapper.rightRowRemove(21);
                verify(listener, times(1)).joinRemoved(20);
            }
        }
    }

    @Nested
    class OuterTests {
        @BeforeEach
        void setUp() {
            mapper = new LookupJoinMapper(interner, listener, 2, 2, true);
        }

        @Nested
        class AddTests {
            @Test
            void leftAddShouldEmitJoinWhenUnmapped() {
                setUpInternerWithMapping(1, -1, 10);
                mapper.leftRowAdd(1);
                verify(listener, times(1)).joinAdded(1);
            }

            @Test
            void rightAddShouldNotEmitJoinWhenUnmapped() {
                setUpInternerWithMapping(-1, 1, 10);
                mapper.rightRowAdd(1);
                verifyNoInteractions(listener);
            }

            @Test
            void leftAddShouldEmitJoinWhenMapped() {
                setUpInternerWithMapping(1, 5, 10);
                mapper.rightRowAdd(5);
                mapper.leftRowAdd(1);
                verify(listener, times(1)).joinAdded(1);
                assertThat(mapper.leftMapper().sourceRowOf(1), equalTo(1));
                assertThat(mapper.rightMapper().sourceRowOf(1), equalTo(5));
            }

            @Test
            void rightAddShouldEmitJoinUpdateWhenMapped() {
                setUpInternerWithMapping(1, 5, 10);
                mapper.leftRowAdd(1);
                verify(listener, times(1)).joinAdded(1);
                mapper.rightRowAdd(5);
                verify(listener, times(1)).joinUpdated(1, false, true);
                assertThat(mapper.leftMapper().sourceRowOf(1), equalTo(1));
                assertThat(mapper.rightMapper().sourceRowOf(1), equalTo(5));
            }
        }

        @Nested
        class ChangeTests {
            @BeforeEach
            void setUp() {
                // mapped pair
                setUpInternerWithMapping(20, 21, 2);
                mapper.leftRowAdd(20);
                mapper.rightRowAdd(21);
                setUpInternerWithMapping(22, 23, 5);
                mapper.leftRowAdd(22);
                mapper.rightRowAdd(23);
                // only left
                setUpInternerWithMapping(30, -1, 3);
                mapper.leftRowAdd(30);
                // only right
                setUpInternerWithMapping(-1, 40, 4);
                mapper.rightRowAdd(40);
                reset(listener);
            }

            @Test
            void leftChangeShouldEmitUpdateWhenMapped() {
                mapper.leftRowChange(20, false);
                verify(listener, times(1)).joinUpdated(20, false, false);
            }

            @Test
            void rightChangeShouldEmitUpdateWhenMapped() {
                mapper.rightRowChange(21, false);
                verify(listener, times(1)).joinUpdated(20, false, false);
            }

            @Test
            void leftChangeShouldEmitUpdateWhenUnmapped() {
                mapper.leftRowChange(30, false);
                verify(listener, times(1)).joinUpdated(30, false, false);
            }

            @Test
            void rightChangeShouldNotEmitUpdateWhenUnmapped() {
                mapper.rightRowChange(40, false);
                verifyNoInteractions(listener);
            }

            @Test
            void rightKeyChangeShouldUpdateOldAndNewJoinsWhenLeftForBothKeys() {
                setUpInternerWithMapping(20, -1, 2);
                setUpInternerWithMapping(30, 21, 3);
                mapper.rightRowChange(21, true);
                verify(listener, times(1)).joinUpdated(20, false, true);
                verify(listener, times(1)).joinUpdated(30, false, true);
            }

            @Test
            void leftKeyChangeShouldEmitJoinUpdateWhenOldKeyUnmappedAndNewKeyMapped() {
                setUpInternerWithMapping(30, 40, 4);
                mapper.leftRowChange(30, true);
                verify(listener, times(1)).joinUpdated(30, false, true);
            }

            @Test
            void leftKeyChangeShouldEmitJoinUpdateWhenOldKeyMappedAndNewKeyUnmapped() {
                setUpInternerWithMapping(20, -1, 10);
                mapper.leftRowChange(20, true);
                verify(listener, times(1)).joinUpdated(20, false, true);
            }

            @Test
            void leftKeyChangeShouldEmitJoinUpdateWhenOldKeyMappedAndNewKeyMapped() {
                setUpInternerWithMapping(20, 40, 4);
                mapper.leftRowChange(20, true);
                verify(listener, times(1)).joinUpdated(20, false, true);
            }
        }

        @Nested
        class RemoveTests {
            @BeforeEach
            void setUp() {
                // mapped pair
                setUpInternerWithMapping(20, 21, 2);
                mapper.leftRowAdd(20);
                mapper.rightRowAdd(21);
                // only left
                setUpInternerWithMapping(30, -1, 3);
                mapper.leftRowAdd(30);
                // only right
                setUpInternerWithMapping(-1, 40, 4);
                mapper.rightRowAdd(40);
                reset(listener);
            }

            @Test
            void leftRemoveShouldRemoveJoinWhenUnmapped() {
                mapper.leftRowRemove(30);
                verify(listener, times(1)).joinRemoved(30);
            }

            @Test
            void rightRemoveShouldNotRemoveJoinWhenUnmapped() {
                mapper.rightRowRemove(40);
                verifyNoInteractions(listener);
            }

            @Test
            void leftRemoveShouldRemoveJoinWhenMapped() {
                mapper.leftRowRemove(20);
                verify(listener, times(1)).joinRemoved(20);
            }

            @Test
            void rightRemoveShouldUpdateJoinWhenMapped() {
                mapper.rightRowRemove(21);
                verify(listener, times(1)).joinUpdated(20, false, true);
            }
        }
    }

    private void setUpInternerWithMapping(final int leftRow, final int rightRow, final int key) {
        if (leftRow != -1) {
            when(left.intern(leftRow)).thenReturn(key);
        }
        if (rightRow != -1) {
            when(right.intern(rightRow)).thenReturn(key);
        }
    }
}

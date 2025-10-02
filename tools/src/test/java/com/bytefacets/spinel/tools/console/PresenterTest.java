// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.tools.console;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bytefacets.spinel.printer.ValueRenderer;
import com.bytefacets.spinel.schema.TypeId;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
class PresenterTest {
    private @Mock ValueRenderer renderer0;
    private @Mock ValueRenderer renderer1;
    private @Mock ValueRenderer renderer2;
    private @Mock Consumer<String> emitter;
    private @Captor ArgumentCaptor<String> outputCaptor;
    private Presenter presenter;

    @BeforeEach
    void setUp() {
        presenter = new Presenter(emitter);
    }

    @Test
    void shouldCalculateValueWidth() {
        assertThat(Presenter.width("123456"), equalTo(8));
        assertThat(Presenter.width(null), equalTo(6));
    }

    @Test
    void shouldResetStringBuilder() {
        presenter.ansi().append("this is a test");
        presenter.update();
        presenter.update();
        // only once because empty string is avoided
        verify(emitter, times(1)).accept(outputCaptor.capture());
        assertThat(outputCaptor.getValue(), equalTo("this is a test"));
    }

    @Nested
    class HeaderTests {
        @BeforeEach
        void setUp() {
            init();
        }

        @Test
        void shouldRenderHeadersWhileMapping() {
            presenter.update();
            verify(emitter, times(1)).accept(outputCaptor.capture());
            assertEquals(
                    String.format(" %-5s  %6s  %4s ", "abc", "abcd", "ab"),
                    outputCaptor.getValue());
        }

        @Test
        void shouldRenderHeaders() {
            presenter.update();
            reset(emitter);

            presenter.renderHeader();
            presenter.update();

            verify(emitter, times(1)).accept(outputCaptor.capture());
            assertEquals(
                    String.format(" %-5s  %6s  %4s ", "abc", "abcd", "ab"),
                    outputCaptor.getValue());
        }
    }

    @Nested
    class CalculateWidthTests {
        @BeforeEach
        void setUp() {
            init();
            presenter.update();
            reset(emitter);
        }

        @Test
        void shouldCalculateSingleFieldWidth() {
            presenter.calculateColumnWidth(5, 1);
            verifyNoInteractions(renderer0, renderer2);
            verify(renderer1, times(1)).render(any(), eq(5));
        }

        @Test
        void shouldSignalRepaintWithLargerWidth() {
            withValue(renderer1, "0".repeat(5));
            assertThat(presenter.calculateColumnWidth(5, 1), equalTo(true));
            withValue(renderer1, "0".repeat(6));
            assertThat(presenter.calculateColumnWidth(5, 1), equalTo(true));
            withValue(renderer1, "0".repeat(5));
            assertThat(presenter.calculateColumnWidth(5, 1), equalTo(false));
        }
    }

    @Nested
    class RowTests {
        @BeforeEach
        void setUp() {
            init();
            presenter.update();
            reset(emitter);
        }

        @Test
        void shouldRenderRow() {
            withValue(renderer0, "12");
            withValue(renderer1, "34");
            withValue(renderer2, "5");

            presenter.renderRow(3);
            presenter.update();

            verify(emitter, times(1)).accept(outputCaptor.capture());
            assertEquals(
                    String.format(" %-5s  %6s  %4s ", "12", "34", "5"), outputCaptor.getValue());
        }
    }

    private void withValue(final ValueRenderer renderer, final String value) {
        doAnswer(
                        inv -> {
                            inv.getArgument(0, StringBuilder.class).append(value);
                            return null;
                        })
                .when(renderer)
                .render(any(), anyInt());
    }

    private void init() {
        presenter.initialize(3);
        presenter.initializeField("abc", 0, TypeId.String, renderer0);
        presenter.initializeField("abcd", 1, TypeId.Int, renderer1);
        presenter.initializeField("ab", 2, TypeId.Long, renderer2);
    }
}
